from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "units", F.col("revenue").cast("int").alias("revenue_int"))
)
