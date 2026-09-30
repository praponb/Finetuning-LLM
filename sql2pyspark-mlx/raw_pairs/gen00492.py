from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"),
    )
)
