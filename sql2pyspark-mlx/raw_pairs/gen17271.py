from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com") & (F.year("signup_date") == 2025))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum("age").alias("total_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)
