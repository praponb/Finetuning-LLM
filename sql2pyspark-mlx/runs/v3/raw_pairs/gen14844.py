from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .filter(~((F.col("s.delivered_date") >= "2025-08-01") & F.col("o.channel").isin(["mobile", "partner"])))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("o.order_date"),
        F.col("c.segment"),
        F.col("c.city"),
    )
    .orderBy(F.col("shipped_date").desc())
)
