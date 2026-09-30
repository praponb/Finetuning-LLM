from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.carrier").isin(["Kerry", "DHL"]))
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
)
