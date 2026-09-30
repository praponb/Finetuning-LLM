from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.store_id").isNull())
    .groupBy(
        F.col("o.status"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
)
