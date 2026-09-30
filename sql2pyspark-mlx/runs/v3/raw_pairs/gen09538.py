from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5, 6]))
    .withColumn("sale_date_month", F.month("sale_date"))
)
