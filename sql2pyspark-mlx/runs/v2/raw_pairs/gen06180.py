from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() | F.col("store_id").isin([1, 3, 4]))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
    .orderBy(F.col("product_id").desc(), "id")
    .limit(20)
)
