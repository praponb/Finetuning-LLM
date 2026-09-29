from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(~F.col("channel").isin(["store", "mobile"])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.order_id").isin([12, 30]))
    .select(F.col("p.id"), F.col("p.paid_at"))
    .orderBy(F.col("id").desc(), "paid_at")
)
