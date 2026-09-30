from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "*",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("year", "started_at_date").alias("year_start"), "customer_id")
    .agg(
        F.min("started_at").alias("first_seen"),
        F.countDistinct("country").alias("distinct_countries"),
        F.max("started_at").alias("last_seen"),
    )
)
