from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("shipping_fee") >= 24).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("p.method").isin(["bank_transfer", "card"]))
    .select(F.col("p.id"), F.col("p.amount"))
)
