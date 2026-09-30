from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5]) | (F.col("sale_date") < "2025-04-12"))
    .groupBy("product_id", "store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 80)
    .orderBy(F.col("product_id").desc())
)
