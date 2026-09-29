from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-04-01")
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("ct.country_list"),
        F.col("ct.country_values"),
    )
)
