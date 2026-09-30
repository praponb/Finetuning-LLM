from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.email").like("%@gmail.com"))
    .groupBy(
        F.when(F.col("c.segment").isin(["SMB", "Consumer"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg(F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"))
    .orderBy(F.col("avg_shipping_fee").desc(), "segment_group")
    .limit(5)
)
