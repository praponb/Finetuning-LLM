from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .intersect(df_payments.select(F.col("amount").alias("price")))
)
