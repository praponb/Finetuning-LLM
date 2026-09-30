from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.store_id"))
    .orderBy("store_id", "id")
    .limit(50)
)
