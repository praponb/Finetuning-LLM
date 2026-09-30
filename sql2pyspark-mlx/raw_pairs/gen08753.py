from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.amount") >= 620)
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("id").desc())
)
