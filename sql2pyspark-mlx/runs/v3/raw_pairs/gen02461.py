from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.city").isin(["Bangkok", "New York"]), "priority").otherwise("other").alias("city_group"),
        F.col("s.city"),
    )
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy("max_amount", F.col("city").desc(), "city_group")
    .limit(10)
)
