from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_claimed_amount_tier_policy_i = Window.partitionBy("claimed_amount_tier", "policy_id").orderBy("claim_date")
w_claimed_amount_tier_claim_da = Window.partitionBy("claimed_amount_tier", F.month("claim_date"))
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_all_by_claim_date_cume_dist = Window.orderBy(F.col("claim_date_cume_dist").desc(), "id")

recent = (
    df_claims
    .filter((F.col("approved_amount") <= 25402.3) & (F.hour(F.col("reported_at")).between(9, 14)))
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
        F.when(F.col("claimed_amount") >= 29985.9, "large").when(F.col("claimed_amount") >= 20436.5, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

windowed = (
    recent
    .withColumn("rows_in_status", F.count(F.lit(1)).over(w_status))
    .withColumn("claim_date_cume_dist", F.round(F.cume_dist().over(w_claimed_amount_tier_policy_i), 3))
)

qualified = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount").between(6745.1, 42447.2)), "left_anti")
    .filter(F.col("adjuster_id").isNotNull())
)

ranked = (
    qualified
    .select(
        "*",
        F.size(F.collect_set("status").over(w_claimed_amount_tier_claim_da)).alias("distinct_status_in_claimed_amount_tier_claim_dat"),
        F.count(F.lit(1)).over(w_claimed_amount_tier_claim_da).alias("rows_in_claimed_amount_tier_claim_date_month"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.sum("claim_date_cume_dist").over(w_claimed_amount_tier_claim_da).alias("claimed_amount_tier_claim_date_month_sum_claim_d"),
        F.lead("claimed_amount").over(w_all_by_claim_date_cume_dist).alias("next_claimed_amount"),
    )
)

result = (
    ranked
    .select(
        "id",
        F.col("rows_in_status").alias("rows_in_status_v"),
        F.col("distinct_status_in_claimed_amount_tier_claim_dat").alias("m_distinct_status_in_claimed_amount_tier_claim_d"),
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_status_v", "m_distinct_status_in_claimed_amount_tier_claim_d"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
