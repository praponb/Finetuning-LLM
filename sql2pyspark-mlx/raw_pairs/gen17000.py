from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("payments.method")))).alias("method_values"),
        F.sum(F.when(F.col("payments.method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
    .orderBy(F.col("cash_count").desc())
)
