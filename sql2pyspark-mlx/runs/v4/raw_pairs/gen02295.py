from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("order_id").isNull()).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.discount") <= 0.11)
    .select(F.col("o.id"), F.col("o.status"))
)
