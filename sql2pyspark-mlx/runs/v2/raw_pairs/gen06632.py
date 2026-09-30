from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 1500)
    .select("id", "store_id", "sale_date", F.floor("revenue").alias("revenue_whole"))
)
