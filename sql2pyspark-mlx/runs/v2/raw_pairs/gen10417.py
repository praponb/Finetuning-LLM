from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(F.col("c.email"), F.col("ct.status_list"))
    .orderBy("email")
)
