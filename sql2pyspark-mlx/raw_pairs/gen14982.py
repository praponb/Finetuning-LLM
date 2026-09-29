from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price", F.lit("order_item").alias("kind"))
    .unionByName(
        df_payments
        .select(F.col("amount").alias("unit_price"), F.lit("payment").alias("kind"))
    )
)
