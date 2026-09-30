from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-08-01")
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "inner")
    .select(F.col("c.signup_date"), F.col("c.age"), F.col("S.country_list"))
    .orderBy(F.col("age").desc())
)
