from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.order_date") < "2025-02-01") & (F.col("p.status") != "SUCCESS"))
    .select(F.col("p.order_id"), F.col("p.method"), F.col("o.shipping_fee"))
)
