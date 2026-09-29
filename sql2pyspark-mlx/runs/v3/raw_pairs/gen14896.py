from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.hour("started_at").between(8, 13))
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.is_active"), F.col("ct.country_list"))
)
