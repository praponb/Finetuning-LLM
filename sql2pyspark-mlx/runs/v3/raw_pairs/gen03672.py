from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 120)
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(
        df_order_items
        .filter(F.col("product_id").isin([3, 36]))
        .select("unit_price", F.lit("order_item").alias("source"))
    )
    .distinct()
)
