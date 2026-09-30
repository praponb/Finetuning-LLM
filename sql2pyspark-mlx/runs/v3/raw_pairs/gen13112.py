from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["West", "North"]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.ceil(F.col("c.credit_limit")).alias("credit_limit_whole"),
    )
    .orderBy(F.col("email").desc(), "id")
    .limit(5)
)
