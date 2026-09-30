from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.hour("paid_at").between(8, 14)).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.status"), F.col("o.discount"), F.col("o.channel"))
)
