from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2026-11-25").cast("date"))) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

per_group = (
    cleaned
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "holder_id",
    )
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.max("start_date").alias("last_start_date"),
    )
)

with_details = (
    per_group.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name", F.col("p.risk_score").alias("holder_risk_score"))
)

result = (
    with_details
    .select(
        "end_date_month",
        "start_date_month",
        "holder_id",
        "median_premium",
        "last_start_date",
        "holder_name",
        "holder_risk_score",
        F.abs(F.col("median_premium") - 2120.0).alias("median_premium_distance"),
    )
    .orderBy(
        F.col("holder_name").asc(),
        "end_date_month",
        "start_date_month",
        "holder_id",
    )
    .limit(5)
)
