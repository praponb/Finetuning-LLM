from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-06-28")))
    .select(
        "*",
        F.floor("revenue").alias("revenue_whole"),
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["city", "region"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("dim.region").alias("region"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.units")).alias("max_units"),
        F.round(F.sum(F.col("a.revenue_whole")), 2).alias("total_revenue_whole"),
        F.grouping_id().alias("gid"),
    )
)
