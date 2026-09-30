from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_reported_at_month = Window.partitionBy(F.month("reported_at"))
w_status_by_approved_amount_pr = Window.partitionBy("status").orderBy("approved_amount", "id").rowsBetween(-2, -1)

cleaned = (
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
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

latest_per_key = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .withColumn("rows_in_reported_at_month", F.count(F.lit(1)).over(w_reported_at_month))
    .withColumn("approved_amount_max_2_prior", F.max("approved_amount").over(w_status_by_approved_amount_pr))
)

result = (
    windowed
    .groupBy("adjuster_id", "reported_at")
    .pivot("claim_type", ["flood", "collision", "medical"])
    .agg(F.avg("rows_in_reported_at_month"))
    .orderBy("adjuster_id", "reported_at")
)
