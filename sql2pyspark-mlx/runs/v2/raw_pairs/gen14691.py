from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .filter(F.col("o.discount").between(0.25, 0.38))
    .groupBy(
        F.when(F.col("o.discount") >= 0.32, "high").when(F.col("o.discount") >= 0.14, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.countDistinct(F.col("s.cost")).alias("unique_carriers"),
    )
    .filter(F.col("unique_carriers") >= 2)
)
