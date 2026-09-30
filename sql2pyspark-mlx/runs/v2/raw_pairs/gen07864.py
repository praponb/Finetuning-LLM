from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.channel") != "partner")
    .select(
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("c.is_active"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("sqft").desc())
)
