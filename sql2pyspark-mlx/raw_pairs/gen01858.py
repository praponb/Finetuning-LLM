from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("channel").isin(["partner", "web", "store"])).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("p.amount").between(480, 1500))
    .select(F.col("p.id"), F.col("p.paid_at"), F.col("p.status"), F.col("p.amount"))
)
