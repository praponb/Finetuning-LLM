from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.store_name").like("%River%"))
    .select(
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
    )
)
