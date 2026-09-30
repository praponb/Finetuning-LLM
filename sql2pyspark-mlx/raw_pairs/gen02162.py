from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(
        F.avg(F.col("p.amount")).alias("avg_amount"),
        F.countDistinct(F.col("p.amount")).alias("unique_orders"),
    )
)
