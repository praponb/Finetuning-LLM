from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.year("order_date") >= 2023)
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.segment"), F.col("t.status_list"))
    .orderBy("segment", F.col("id").desc())
)
