from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("p.status") == "SUCCESS", F.col("p.amount")).otherwise(0)).alias("success_amount"),
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
    )
    .orderBy("success_amount", "order_month")
)
