from pyspark.sql import functions as F

result = (
    df_transactions
    .select(F.col("amount").alias("value"))
    .unionByName(df_shipments.select(F.col("cost").alias("value")))
    .distinct()
    .orderBy("value")
)
