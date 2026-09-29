from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count("*").alias("num_payments"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("payments.method")))).alias("method_values"),
    )
    .orderBy(F.col("num_payments").desc(), "channel")
    .limit(5)
)
