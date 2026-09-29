from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.year(F.col("s.delivered_date")) == 2025) & F.col("o.customer_id").isNull())
    .select(
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("o.status"),
    )
)
