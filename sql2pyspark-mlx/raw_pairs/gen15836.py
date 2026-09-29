from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 260)
    .select(F.lit("order_item").alias("record_type"), "unit_price")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("record_type"), "monthly_fee")
    )
    .orderBy(F.col("record_type").desc())
)
