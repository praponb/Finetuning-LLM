from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6]) & (F.col("product_id") == 7))
    .withColumn("sale_date_period", F.date_trunc("week", "sale_date"))
)
