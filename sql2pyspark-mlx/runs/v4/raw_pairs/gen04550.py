from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("C.channel_list"))
)
