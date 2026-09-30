from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.hour("paid_at").between(12, 15)).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.store_id").isNull())
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.store_id"))
)
