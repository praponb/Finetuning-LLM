from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("channel").isin(["web", "partner", "mobile"])).select("id")

result = (
    df_payments.alias("payments")
    .join(excluded_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("payments.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(F.col("payments.id"), F.col("payments.method"), F.col("payments.order_id"))
    .orderBy("id")
)
