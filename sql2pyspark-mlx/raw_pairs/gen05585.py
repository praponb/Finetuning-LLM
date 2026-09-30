from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-07-23"))) & F.col("customer_id").isNotNull())
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
)

result = (
    filtered
    .rollup("device")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("country").alias("distinct_countries_2"),
        F.max("started_at").alias("ended_at"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("row_count") < 1000)
)
