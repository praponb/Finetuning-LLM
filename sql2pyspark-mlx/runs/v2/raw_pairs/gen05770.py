from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("channel").isin(["store", "web"])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.amount").between(500, 1400))
    .select(F.col("p.id"), F.col("p.method"))
)
