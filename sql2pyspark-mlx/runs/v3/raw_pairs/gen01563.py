from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("s.city").isin(["Bangkok", "Tokyo"]) & (F.col("o.amount") > 1300))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("s.region"),
        F.col("c.country"),
        F.col("c.email"),
    )
    .orderBy(F.col("channel").desc(), "id")
    .limit(20)
)
