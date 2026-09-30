from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("shipping_fee").alias("amount"), F.lit("order").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
