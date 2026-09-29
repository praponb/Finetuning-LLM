from pyspark.sql import functions as F

filtered_customers = df_customers.filter((F.year("signup_date") >= 2025) & F.col("is_active"))

summary = (
    filtered_customers
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    summary
    .select("segment", "signup_month", "country_values")
    .orderBy("segment")
)
