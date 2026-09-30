from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-09-04").cast("date"))
    .select(
        "*",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.lower("status").alias("status_lower"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("plan", ["basic", "standard"])
    .agg(F.max("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("n"))
)
