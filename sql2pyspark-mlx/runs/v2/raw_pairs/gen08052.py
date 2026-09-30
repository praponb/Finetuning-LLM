from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("s.device").isin(["mobile", "desktop", "tablet"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.is_active"))
    .agg(
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.country")))).alias("country_values"),
    )
    .orderBy(F.col("unique_customers").desc())
)
