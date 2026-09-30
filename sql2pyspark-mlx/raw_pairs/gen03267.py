from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1200)
    .select(F.lit("store_sale").alias("kind"), "revenue")
    .union(df_payments.select(F.lit("payment").alias("kind"), "amount"))
)
