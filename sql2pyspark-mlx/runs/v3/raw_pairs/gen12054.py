from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"), F.lit("payment").alias("source"))
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(110, 390))
        .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("source"))
    )
    .distinct()
)
