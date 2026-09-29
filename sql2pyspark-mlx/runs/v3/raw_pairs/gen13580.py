from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4, 6, 9]) & (F.col("revenue").isNotNull() | (F.col("revenue") < 2500)))
)

summary = (
    src
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    summary
    .filter(F.col("total_units") >= 130)
    .orderBy("product_id", "total_units")
)
