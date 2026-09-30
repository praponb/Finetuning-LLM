from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~(~F.col("c.city").isin(["Chiang Mai", "Berlin", "London"]) & F.col("c.credit_limit").between(9000, 12500)))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
    .orderBy("city")
)
