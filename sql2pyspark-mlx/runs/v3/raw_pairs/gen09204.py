from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("amount").isNull()).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.status"), F.col("o.amount"))
)
