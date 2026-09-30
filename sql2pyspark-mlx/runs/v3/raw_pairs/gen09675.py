from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(~((F.col("revenue") > 3600) & F.col("store_id").isin([1, 2, 3, 5])))
)

store_sales_stats = (
    src
    .groupBy(
        F.when(F.col("units") >= 37, "high").when(F.col("units") >= 15, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("units").alias("max_units"),
    )
)

result = (
    store_sales_stats
    .select("units_bucket", "row_count", "max_units")
    .orderBy("row_count")
)
