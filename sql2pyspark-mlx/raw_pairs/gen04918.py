from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") >= 4500)
    .groupBy(
        F.when(F.col("c.segment").isin(["Corporate", "SMB"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg(F.count(F.col("o.discount")).alias("discount_count"))
    .orderBy("segment_group", F.col("discount_count").desc())
)
