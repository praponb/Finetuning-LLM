from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("revenue") >= 1500)
    .select("*", F.floor("revenue").alias("revenue_whole"))
)

result = (
    filtered
    .groupBy(F.date_trunc("quarter", "sale_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.min("units").alias("min_units"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("event_count").desc())
)
