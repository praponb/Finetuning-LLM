from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 7, 9]))
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
)
