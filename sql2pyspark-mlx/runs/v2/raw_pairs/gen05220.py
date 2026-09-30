from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .filter(F.col("p.order_id").isNotNull())
    .join(df_orders.filter(F.month("order_date").isin([3, 6, 11])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("p.id"), F.col("p.method"))
)
