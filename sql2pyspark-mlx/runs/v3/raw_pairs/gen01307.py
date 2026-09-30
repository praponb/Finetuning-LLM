from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_methods"))
    .orderBy("unique_methods")
)
