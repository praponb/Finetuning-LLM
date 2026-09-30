from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .groupBy(
        "is_active",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") >= 8)
)
