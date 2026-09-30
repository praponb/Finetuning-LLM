from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.status").isin(["SHIPPED", "RETURNED", "PENDING"]))
    .select(
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.floor(F.col("s.cost")).alias("cost_whole"),
    )
    .orderBy(F.col("cost_whole").desc())
)
