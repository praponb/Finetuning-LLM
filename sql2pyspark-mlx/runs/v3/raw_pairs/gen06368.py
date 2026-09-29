from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.month("order_date").isin([3, 11, 12])).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(F.col("p.id"), F.col("p.amount"))
)
