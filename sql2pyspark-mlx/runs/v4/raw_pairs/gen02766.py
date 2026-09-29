from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("p.amount").isNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .orderBy("channel")
)
