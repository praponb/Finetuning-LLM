from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.customer_id") == 34)
    .groupBy(F.col("o.channel"))
    .agg(
        F.sort_array(F.collect_set(F.col("p.method"))).alias("method_list"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
)
