from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", F.lit("order").alias("source"))
    .union(
        df_order_items
        .filter(F.col("order_id").isin([20, 21, 26, 36]))
        .select("unit_price", F.lit("order_item").alias("source"))
    )
    .distinct()
)
