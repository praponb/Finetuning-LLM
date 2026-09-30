from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.amount") > 970)
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)
