from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("sbc.country_list"))
)
