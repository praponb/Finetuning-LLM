from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 9]))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)
