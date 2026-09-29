from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.channel") == "store")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("amount").desc(), "status", "id")
    .limit(50)
)
