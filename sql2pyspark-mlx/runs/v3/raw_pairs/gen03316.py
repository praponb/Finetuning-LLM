from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.channel").isin(["store", "partner"]))
    .select(
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
        F.col("c.segment"),
        F.col("c.email"),
    )
    .orderBy(F.col("sqft").desc(), F.col("shipping_fee").desc())
)
