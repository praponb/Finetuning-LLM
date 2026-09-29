from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(~F.col("p.status").isin(["FAILED", "SUCCESS"]))
    .groupBy(F.col("o.status"))
    .agg(F.count(F.col("p.id")).alias("payment_count"))
    .filter(F.col("payment_count") >= 5)
)
