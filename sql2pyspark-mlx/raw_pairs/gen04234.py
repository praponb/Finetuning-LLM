from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([21, 22, 30, 33]))
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.country")))).alias("country_values"),
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
    )
)
