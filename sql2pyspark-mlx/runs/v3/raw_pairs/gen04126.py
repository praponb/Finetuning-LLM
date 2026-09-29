from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .unionByName(df_shipments.select(F.col("cost").alias("amount")))
    .orderBy("amount")
)
