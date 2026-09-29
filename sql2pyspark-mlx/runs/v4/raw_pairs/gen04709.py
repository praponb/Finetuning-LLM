from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.city"))
    .agg(F.sort_array(F.collect_set(F.col("s.country"))).alias("country_list"))
)
