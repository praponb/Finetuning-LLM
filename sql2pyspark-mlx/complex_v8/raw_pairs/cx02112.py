from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_policy_product_by = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_claim_type_or_un = Window.partitionBy("adjuster_id", "claim_type_or_unknown").orderBy(F.col("policy_end_date").desc_nulls_first(), "id")
w_all_by_policy_end_date_ties = Window.orderBy(F.col("policy_end_date").desc_nulls_first())
w_claim_type_or_unknown_by_pol = Window.partitionBy("claim_type_or_unknown").orderBy(F.col("policy_end_date").asc_nulls_last(), "id")
w_claim_type_or_unknown_policy = Window.partitionBy("claim_type_or_unknown", "policy_id").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_policy_id_policy_product_by_2 = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_or_unknown_policy_2 = Window.partitionBy("claim_type_or_unknown", "policy_id").orderBy(F.col("reported_at").asc_nulls_last())
w_status_by_policy_end_date = Window.partitionBy("status").orderBy(F.col("policy_end_date").desc(), "id")
w_policy_id_policy_product_by_3 = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at")
w_status_by_policy_end_date_ti = Window.partitionBy("status").orderBy(F.col("policy_end_date").desc())
w_policy_id_policy_product_by_4 = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at", "id")
w_claim_type_or_unknown_policy_3 = Window.partitionBy("claim_type_or_unknown", "policy_end_date_quartile")
w_all_by_next_reported_at_ties = Window.orderBy("next_reported_at")
w_all_by_prev2_policy_end_date = Window.orderBy("prev2_policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_reported_at = Window.orderBy("next_reported_at", "id")
w_all_by_next_reported_at_runn = Window.orderBy("next_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_policy_product_in_c = Window.partitionBy("distinct_policy_product_in_claim_type_or_unknown").orderBy(F.col("last_next_reported_at").asc_nulls_last(), "distinct_policy_product_in_claim_type_or_unknown")
w_distinct_policy_product_in_c_2 = Window.partitionBy("distinct_policy_product_in_claim_type_or_unknown")

cleaned_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

combined_details_policy = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.claim_type_or_unknown",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
    )
)

sequenced = (
    combined_details_policy
    .select(
        "id",
        "status",
        "reported_at",
        "claim_type_or_unknown",
        "policy_end_date",
        "policy_product",
        F.first("policy_end_date", ignorenulls=True).over(w_policy_id_policy_product_by).alias("first_known_policy_end_date"),
        F.ntile(4).over(w_adjuster_id_claim_type_or_un).alias("policy_end_date_quartile"),
        F.rank().over(w_all_by_policy_end_date_ties).alias("policy_end_date_rank"),
        F.rank().over(w_claim_type_or_unknown_by_pol).alias("policy_end_date_rank_2"),
        F.dense_rank().over(w_claim_type_or_unknown_policy).alias("reported_at_dense_rank"),
        F.nth_value("reported_at", 2).over(w_policy_id_policy_product_by_2).alias("second_reported_at"),
        F.dense_rank().over(w_claim_type_or_unknown_policy_2).alias("reported_at_dense_rank_2"),
        F.lag("policy_end_date").over(w_status_by_policy_end_date).alias("prev_policy_end_date"),
        F.round(F.cume_dist().over(w_policy_id_policy_product_by_3), 3).alias("reported_at_cume_dist"),
        F.rank().over(w_status_by_policy_end_date_ti).alias("policy_end_date_rank_3"),
        F.lead("reported_at").over(w_adjuster_id_claim_type_or_un).alias("next_reported_at"),
        F.round(F.cume_dist().over(w_status_by_policy_end_date_ti), 3).alias("policy_end_date_cume_dist"),
        F.lag("policy_end_date", 2).over(w_policy_id_policy_product_by_4).alias("prev2_policy_end_date"),
        F.lead("policy_end_date").over(w_policy_id_policy_product_by_4).alias("next_policy_end_date"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        F.coalesce(F.col("reported_at_dense_rank"), F.lit(0)).alias("reported_at_dense_rank_filled"),
        F.date_format(F.col("next_reported_at"), "yyyy-MM-dd").alias("next_reported_at_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.size(F.collect_set("status").over(w_claim_type_or_unknown_policy_3)).alias("distinct_status_in_claim_type_or_unknown_policy_"),
        F.max("policy_end_date_cume_dist").over(w_claim_type_or_unknown_policy_3).alias("claim_type_or_unknown_policy_end_date_quartile_m"),
        F.round(F.cume_dist().over(w_all_by_next_reported_at_ties), 3).alias("next_reported_at_cume_dist"),
        F.last("policy_end_date").over(w_all_by_prev2_policy_end_date).alias("last_policy_end_date"),
        F.round(F.col("policy_end_date_rank_3") - F.avg("policy_end_date_rank_3").over(w_claim_type_or_unknown_policy_3), 2).alias("policy_end_date_rank_3_vs_claim_type_or_unknown_"),
        F.size(F.collect_set("policy_product").over(w_claim_type_or_unknown_policy_3)).alias("distinct_policy_product_in_claim_type_or_unknown"),
        F.datediff(F.col("policy_end_date"), F.lag("policy_end_date").over(w_all_by_next_reported_at)).alias("days_since_prev_policy_end_date"),
        F.last("second_reported_at", ignorenulls=True).over(w_all_by_next_reported_at_runn).alias("last_known_second_reported_at"),
    )
)

totals = (
    windowed
    .filter(F.col("claim_type_or_unknown") != "delay")
    .groupBy("distinct_policy_product_in_claim_type_or_unknown")
    .agg(
        F.round(F.avg("reported_at_dense_rank"), 2).alias("avg_reported_at_dense_rank"),
        F.max("next_reported_at").alias("last_next_reported_at"),
    )
)

ranked = (
    totals
    .select(
        "*",
        F.round(F.cume_dist().over(w_distinct_policy_product_in_c), 3).alias("last_next_reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_policy_product_in_c_2).alias("rows_in_distinct_policy_product_in_claim_type_or"),
        F.row_number().over(w_distinct_policy_product_in_c).alias("rn"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_distinct_policy_product_in_claim_type_or") <= 13)
    .select(
        "distinct_policy_product_in_claim_type_or_unknown",
        "last_next_reported_at",
        "last_next_reported_at_cume_dist",
        "rows_in_distinct_policy_product_in_claim_type_or",
        "rn",
        F.date_format(F.col("last_next_reported_at"), "yyyy-MM-dd").alias("last_next_reported_at_label"),
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy(
        F.col("distinct_policy_product_in_claim_type_or_unknown").asc_nulls_last(),
    )
)
