from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("p.amount")).alias("min_amount"))
)
