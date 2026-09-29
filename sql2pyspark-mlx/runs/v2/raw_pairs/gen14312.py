from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
