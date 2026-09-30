from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.store_id"), F.col("o.customer_id"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("p.method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
)
