from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.segment").isin(["Corporate", "SMB"]), "group_a").otherwise("other").alias("segment_group"),
    )
    .agg(F.max(F.col("p.paid_at")).alias("latest_paid_at"))
    .orderBy("latest_paid_at", "segment_group")
    .limit(10)
)
