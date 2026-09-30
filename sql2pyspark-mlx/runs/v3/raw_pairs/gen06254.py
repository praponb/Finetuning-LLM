from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.order_date") < "2024-05-01")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("s.store_name"),
    )
    .orderBy("channel", "id")
    .limit(20)
)
