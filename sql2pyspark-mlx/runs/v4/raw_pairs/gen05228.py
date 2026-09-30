from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.amount") > 470)
    .groupBy(
        F.when(F.col("orders.amount") >= 1670, "high").when(F.col("orders.amount") >= 560, "mid").otherwise("low").alias("amount_bucket"),
        F.col("orders.customer_id"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("payments.method")))).alias("method_values"),
        F.round(((100 * F.sum(F.when(F.col("payments.method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
)
