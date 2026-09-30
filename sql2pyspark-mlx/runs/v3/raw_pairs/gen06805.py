from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .unionByName(df_shipments.select(F.col("cost").alias("amount")))
)
