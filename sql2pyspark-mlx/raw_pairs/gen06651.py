from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-08-30").cast("date"))
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .cube(F.col("d.segment").alias("segment"), F.col("e.browser").alias("browser"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.days_since_started_at")).alias("min_days_since_started_at"),
    )
)
