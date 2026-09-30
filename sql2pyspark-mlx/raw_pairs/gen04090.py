from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("credit_limit") < 5000) & (~F.col("name").like("%Tayl%") | F.col("city").isin(["Chiang Mai", "Berlin", "Tokyo"])))
)

customers_stats = (
    filtered_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "country")
    .agg(
        F.min("age").alias("min_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = customers_stats.select("signup_quarter", "country", "min_age", "city_list")
