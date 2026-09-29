from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("s.sqft"),
        F.col("s.city"),
    )
)
