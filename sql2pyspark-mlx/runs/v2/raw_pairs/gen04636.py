from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("browser")).alias("browser_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("ct.browser_list"))
    .orderBy("id")
)
