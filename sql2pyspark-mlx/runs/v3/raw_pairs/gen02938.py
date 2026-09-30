from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser").isin(["Firefox", "Edge"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
        F.count(F.when(F.col("s.country") == "TH", 1)).alias("th_count"),
    )
    .orderBy(F.col("signup_year").desc())
)
