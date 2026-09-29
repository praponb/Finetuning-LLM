from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.month("order_date").isin([3, 9, 11])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.status"), F.col("p.method"))
)
