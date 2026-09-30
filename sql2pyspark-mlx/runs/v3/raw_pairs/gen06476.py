from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.customer_id").isNotNull() & ~F.col("o.channel").isin(["web", "store", "mobile"]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.greatest(F.col("o.discount"), F.col("o.shipping_fee")).alias("greatest_discount_shipping_fee"),
    )
)
