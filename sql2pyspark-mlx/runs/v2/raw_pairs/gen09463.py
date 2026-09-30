from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.method") == "paypal")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
