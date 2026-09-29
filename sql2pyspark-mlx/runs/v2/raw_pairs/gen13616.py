from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
        F.sum(F.when(F.col("p.status") == "REFUNDED", F.col("p.amount")).otherwise(0)).alias("refunded_amount"),
    )
)
