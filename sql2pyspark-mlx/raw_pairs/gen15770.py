from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("status") == "REFUNDED").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.store_id").isin([6, 8, 26]))
    .select(F.col("o.id"), F.col("o.order_date"))
    .orderBy("id")
)
