from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
    )
    .orderBy("status_values", F.col("signup_month").desc())
)
