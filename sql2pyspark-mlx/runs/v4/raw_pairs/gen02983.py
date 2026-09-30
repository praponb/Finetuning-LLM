from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.cost") >= 59)
    .groupBy(
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
        F.col("o.channel"),
    )
    .agg(F.min(F.col("s.shipped_date")).alias("min_shipped_date"))
    .orderBy("min_shipped_date", "order_quarter", "channel")
    .limit(20)
)
