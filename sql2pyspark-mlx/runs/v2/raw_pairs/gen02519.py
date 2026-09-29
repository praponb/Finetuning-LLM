from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("o.status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy("segment")
)
