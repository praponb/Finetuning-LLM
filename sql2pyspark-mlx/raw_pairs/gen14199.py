from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("method").isin(["card", "bank_transfer"])).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.store_id"))
)
