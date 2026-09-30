from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("source"))
    .unionByName(
        df_order_items
        .select(F.col("unit_price").alias("amount"), F.lit("order_item").alias("source"))
    )
    .orderBy("amount")
)
