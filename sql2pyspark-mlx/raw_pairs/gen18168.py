from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-06-26").cast("date"))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .cube(F.col("d.segment").alias("segment"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.monthly_fee")).alias("avg_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
