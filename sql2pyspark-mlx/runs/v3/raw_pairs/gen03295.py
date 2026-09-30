from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .filter(F.col("pct_lost") > 61)
)
