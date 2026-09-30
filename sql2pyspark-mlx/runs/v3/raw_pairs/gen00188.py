from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.store_id") == 18)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("amount", "id")
    .limit(3)
)
