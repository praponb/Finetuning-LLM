from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(F.lit("order_item").alias("origin"), "unit_price")
    .union(
        df_payments
        .filter(F.col("amount").isNotNull())
        .select(F.lit("payment").alias("origin"), "amount")
    )
)
