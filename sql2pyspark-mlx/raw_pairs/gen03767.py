from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select(F.lit("order").alias("kind"), "shipping_fee")
    .union(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select(F.lit("transaction").alias("kind"), "amount")
    )
)
