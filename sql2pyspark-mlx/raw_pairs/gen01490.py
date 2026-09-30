from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-14").cast("date"))
    .select(
        "*",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.when(F.col("units") >= 30, "High").when(F.col("units") >= 9, "Medium").otherwise("Low").alias("units_band"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.revenue")).alias("avg_revenue"),
    )
    .orderBy(F.col("region").desc())
)
