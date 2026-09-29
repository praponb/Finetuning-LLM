from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .filter(F.col("c.country") == "TH")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("c.email"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("id").desc())
)
