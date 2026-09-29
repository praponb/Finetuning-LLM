from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-09-27")
    .select(F.col("amount").alias("value"))
    .unionByName(df_products.select(F.col("cost").alias("value")))
)
