from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_plus_15m_month_b = Window.partitionBy(F.month("reported_at_plus_15m")).orderBy("reported_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_plus_15m_month_b_2 = Window.partitionBy(F.month("reported_at_plus_15m")).orderBy("reported_at_plus_15m")
w_policy_status = Window.partitionBy("policy_status")
w_policy_status_by_adjuster_le = Window.partitionBy("policy_status").orderBy(F.col("adjuster_level").asc_nulls_last())
w_reported_at_plus_15m_month_b_3 = Window.partitionBy(F.month("reported_at_plus_15m")).orderBy("reported_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_plus_15m = Window.orderBy("reported_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_claims
    .filter(F.col("policy_id") >= 5)
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
        F.when(F.col("approved_amount") >= 22250.6, "A").when(F.col("approved_amount") >= 0.0, "B").otherwise("C").alias("approved_amount_tier"),
        (F.col("reported_at") + F.expr("INTERVAL 15 MINUTES")).alias("reported_at_plus_15m"),
    )
)

with_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.lower(F.col("a.adjuster_name")).like("%ca%"))
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

metrics = (
    with_details_adjuster
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_tier",
        "adjuster_name",
        "adjuster_level",
        F.nth_value("reported_at_plus_15m", 2).over(w_reported_at_plus_15m_month_b).alias("second_reported_at_plus_15m"),
        F.round(F.percent_rank().over(w_reported_at_plus_15m_month_b_2), 3).alias("reported_at_plus_15m_percent_rank"),
        F.sum("claimed_amount").over(w_policy_status).alias("policy_status_sum_claimed_amount"),
        F.dense_rank().over(w_policy_status_by_adjuster_le).alias("adjuster_level_dense_rank"),
        F.size(F.collect_set("claim_type").over(w_policy_status)).alias("distinct_claim_type_in_policy_status"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_status), 2).alias("adjuster_level_vs_policy_status_avg"),
        F.count(F.lit(1)).over(w_policy_status).alias("rows_in_policy_status"),
        F.last("policy_premium", ignorenulls=True).over(w_reported_at_plus_15m_month_b_3).alias("last_known_policy_premium"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_status), F.lit(0)), 4).alias("adjuster_level_share_of_policy_status"),
        F.last("reported_at_plus_15m", ignorenulls=True).over(w_all_by_reported_at_plus_15m).alias("last_known_reported_at_plus_15m"),
    )
)

derived = (
    metrics
    .filter(F.col("policy_status_sum_claimed_amount") <= 325035.0)
    .select(
        "*",
        (F.col("adjuster_level") + F.col("rows_in_policy_status")).alias("adjuster_level_plus_rows_in_policy_status"),
    )
)

result = (
    derived
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "adjuster_name",
        "adjuster_level",
        "second_reported_at_plus_15m",
        "reported_at_plus_15m_percent_rank",
        "distinct_claim_type_in_policy_status",
        "rows_in_policy_status",
        "last_known_policy_premium",
        "adjuster_level_share_of_policy_status",
        "last_known_reported_at_plus_15m",
        "adjuster_level_plus_rows_in_policy_status",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.least(F.col("claimed_amount"), F.col("rows_in_policy_status")).alias("least_claimed_amount_rows_in_policy_status"),
    )
    .orderBy(F.col("distinct_claim_type_in_policy_status").desc(), "reported_at")
)
