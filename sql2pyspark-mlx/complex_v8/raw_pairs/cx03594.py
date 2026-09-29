from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_pol = Window.partitionBy("adjuster_id", "policy_id").orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id_by_pol_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("policy_start_date", "id")
w_claim_type_policy_id_by_poli = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("policy_start_date").asc_nulls_last(), "id")
w_all_by_policy_start_date = Window.orderBy(F.col("policy_start_date").asc_nulls_last(), "id")
w_claim_type_policy_id_by_poli_2 = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("policy_start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_policy_start_d = Window.partitionBy("claim_type").orderBy(F.col("policy_start_date").asc_nulls_last())
w_policy_start_date_dense_rank = Window.partitionBy("policy_start_date_dense_rank", "policy_id")
w_claim_type_first_known_polic = Window.partitionBy("claim_type", F.month("first_known_policy_start_date")).orderBy(F.col("next3_policy_start_date").desc(), "id")
w_all_by_next3_policy_start_da = Window.orderBy("next3_policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_policy_holder_id = Window.partitionBy("status", "policy_holder_id")
w_all_by_next3_policy_start_da_2 = Window.orderBy("next3_policy_start_date")

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

combined_details = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2024-06-19").cast("date")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.status_slug",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.first("policy_start_date", ignorenulls=True).over(w_adjuster_id_policy_id_by_pol).alias("first_known_policy_start_date"),
        F.lead("policy_start_date").over(w_adjuster_id_policy_id_by_pol_2).alias("next_policy_start_date"),
        F.row_number().over(w_adjuster_id_policy_id_by_pol_2).alias("seq_no"),
        F.lead("policy_start_date", 3).over(w_claim_type_policy_id_by_poli).alias("next3_policy_start_date"),
        F.round(F.cume_dist().over(w_all_by_policy_start_date), 3).alias("policy_start_date_cume_dist"),
        F.last("policy_start_date", ignorenulls=True).over(w_claim_type_policy_id_by_poli_2).alias("last_known_policy_start_date"),
        F.dense_rank().over(w_claim_type_by_policy_start_d).alias("policy_start_date_dense_rank"),
        F.rank().over(w_claim_type_by_policy_start_d).alias("policy_start_date_rank"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("policy_start_date_rank") <= 5)
    .drop("policy_start_date_rank")
)

windowed = (
    top_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status_slug",
        "policy_start_date",
        "policy_holder_id",
        "first_known_policy_start_date",
        "next_policy_start_date",
        "seq_no",
        "policy_start_date_cume_dist",
        "last_known_policy_start_date",
        "policy_start_date_dense_rank",
        F.size(F.collect_set("claim_type").over(w_policy_start_date_dense_rank)).alias("distinct_claim_type_in_policy_start_date_dense_r"),
        F.ntile(3).over(w_claim_type_first_known_polic).alias("next3_policy_start_date_tercile"),
        F.first("next3_policy_start_date").over(w_all_by_next3_policy_start_da).alias("first_next3_policy_start_date"),
        F.size(F.collect_set("claim_type").over(w_status_policy_holder_id)).alias("distinct_claim_type_in_status_policy_holder_id"),
        F.round(F.col("policy_start_date_dense_rank") - F.avg("policy_start_date_dense_rank").over(w_status_policy_holder_id), 2).alias("policy_start_date_dense_rank_vs_status_policy_ho"),
        F.round(F.cume_dist().over(w_all_by_next3_policy_start_da_2), 3).alias("next3_policy_start_date_cume_dist"),
    )
)

summary = (
    windowed
    .groupBy(
        "claim_type",
        F.date_trunc("month", "policy_start_date").alias("policy_start_date_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("policy_start_date_dense_rank_vs_status_policy_ho").alias("min_policy_start_date_dense_rank_vs_status_polic"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
)

result = (
    summary
    .select(
        "claim_type",
        "policy_start_date_month",
        "cnt",
        "distinct_policy_ids",
        "distinct_adjuster_ids",
        "min_policy_start_date_dense_rank_vs_status_polic",
        "distinct_claim_types",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("distinct_claim_types").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("claim_type_prefix").asc())
)
