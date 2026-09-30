from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.shipping_fee") > 25)
    .groupBy(F.year(F.col("orders.order_date")).alias("order_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("payments.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.count(F.when(F.col("payments.method") == "promptpay", 1)).alias("promptpay_count"),
    )
)
