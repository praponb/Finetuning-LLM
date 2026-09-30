from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.sum(F.col("p.amount")).alias("total_amount"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 610)
)
