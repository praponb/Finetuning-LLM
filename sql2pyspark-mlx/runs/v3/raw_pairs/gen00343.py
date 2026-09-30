from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .select("amount")
    .unionByName(df_products.select(F.col("cost").alias("amount")))
    .distinct()
)
