from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("s.opened_date") < "2025-07-24") & (F.length(F.col("s.store_name")) > 13))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("s.region"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
