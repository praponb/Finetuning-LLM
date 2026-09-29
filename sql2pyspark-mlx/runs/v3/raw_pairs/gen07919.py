from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.discount") >= 0.33, "high").when(F.col("o.discount") >= 0.14, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)
