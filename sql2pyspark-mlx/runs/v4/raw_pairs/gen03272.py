from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_methods"))
    .orderBy("status", F.col("unique_methods").desc())
    .limit(1)
)
