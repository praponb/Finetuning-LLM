from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        F.col("b.sqft"),
    )
)
