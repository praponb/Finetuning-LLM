from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status = Window.partitionBy("policy_status")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_claim_type_group = Window.partitionBy("claim_type", "claim_type_group").orderBy(F.col("reported_at").desc())
w_claim_date_month_claim_type = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_policy_id_in_adjust = Window.partitionBy("distinct_policy_id_in_adjuster_id", F.date_trunc("month", "first_claim_date"))
w_claim_type_by_distinct_polic = Window.partitionBy("claim_type").orderBy(F.col("distinct_policy_id_in_adjuster_id").desc(), "id")
w_all_by_first_claim_date_runn = Window.orderBy(F.col("first_claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_claim_date_2 = Window.orderBy("first_claim_date_2", "id")

valid_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (F.col("claim_type") == "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.when(F.col("claim_type").isin("medical", "theft"), "group_a").when(F.col("claim_type") == "flood", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

with_details_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.claim_type_group",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "claim_type_group",
        "adjuster_team",
        "policy_premium",
        "policy_status",
        F.sum("policy_premium").over(w_policy_status).alias("policy_status_sum_policy_premium"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.round(F.cume_dist().over(w_claim_type_claim_type_group), 3).alias("reported_at_cume_dist"),
        F.max("policy_premium").over(w_adjuster_id).alias("adjuster_id_max_policy_premium"),
        F.first("claim_date").over(w_claim_date_month_claim_type).alias("first_claim_date"),
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.first("claim_date").over(w_all_by_reported_at_full).alias("first_claim_date_2"),
        F.nth_value("claim_date", 2).over(w_all_by_reported_at_full).alias("second_claim_date"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("reported_at_dense_rank") <= 5)
    .drop("reported_at_dense_rank")
)

metrics = (
    top_rows
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "claim_type_group",
        "adjuster_team",
        "policy_premium",
        "policy_status",
        "distinct_policy_id_in_adjuster_id",
        "reported_at_cume_dist",
        "adjuster_id_max_policy_premium",
        "first_claim_date_2",
        "second_claim_date",
        F.count(F.lit(1)).over(w_distinct_policy_id_in_adjust).alias("rows_in_distinct_policy_id_in_adjuster_id_first_"),
        F.lead("first_claim_date", 2).over(w_claim_type_by_distinct_polic).alias("next2_first_claim_date"),
        F.count(F.lit(1)).over(w_all_by_first_claim_date_runn).alias("running_count"),
        F.rank().over(w_all_by_first_claim_date_2).alias("first_claim_date_2_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "claim_date",
        "claim_type",
        "reported_at",
        "claim_type_group",
        "adjuster_team",
        "policy_status",
        "reported_at_cume_dist",
        "adjuster_id_max_policy_premium",
        "first_claim_date_2",
        "second_claim_date",
        "rows_in_distinct_policy_id_in_adjuster_id_first_",
        "next2_first_claim_date",
        "running_count",
        "first_claim_date_2_rank",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.when(F.col("reported_at_cume_dist") >= 0.8, "large").when(F.col("reported_at_cume_dist") >= 0.2, "medium").otherwise("small").alias("reported_at_cume_dist_tier"),
    )
    .orderBy(
        F.col("adjuster_id_max_policy_premium").asc_nulls_last(),
        F.col("rows_in_distinct_policy_id_in_adjuster_id_first_").desc(),
        "id",
    )
    .limit(20)
)
