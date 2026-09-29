from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.pct_success") > 49)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("O.pct_success"),
    )
    .orderBy("shipping_fee")
)
