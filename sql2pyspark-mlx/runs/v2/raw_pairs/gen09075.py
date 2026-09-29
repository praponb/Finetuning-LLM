from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee").isNotNull()).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.status") == "SUCCESS")
    .select(F.col("p.id"), F.col("p.status"))
)
