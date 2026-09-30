from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(~F.col("c.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
)
