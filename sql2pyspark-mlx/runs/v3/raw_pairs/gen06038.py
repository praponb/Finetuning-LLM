from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(F.col("is_active") | F.col("name").like("%Pim%")))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count("age").alias("age_count"),
    )
)
