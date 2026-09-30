from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(F.col("o.channel"))
    .agg(F.count("*").alias("cnt"))
)
