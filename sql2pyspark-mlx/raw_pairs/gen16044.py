from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .select(
        F.col("s.cost"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.date_trunc("WEEK", F.col("o.order_date")).alias("order_date_period"),
    )
    .orderBy("amount")
)
