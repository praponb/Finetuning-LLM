from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 8]))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)
