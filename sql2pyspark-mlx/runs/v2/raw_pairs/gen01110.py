from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.status_list"))
    .orderBy("city")
)
