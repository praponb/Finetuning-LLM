from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("week", "signup_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("cnt"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.countDistinct("country").alias("distinct_countries"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("credit_limit_per_london"),
    )
    .orderBy("credit_limit_per_london")
)
