from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & (F.col("o.discount") < 0.37))
    .select(F.col("o.discount"), F.col("p.amount"))
)
