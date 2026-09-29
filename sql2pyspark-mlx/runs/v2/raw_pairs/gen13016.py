from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .filter(F.col("stddev_amount") > 39)
)
