from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(~(F.upper("email").like("%LIC%") & F.col("is_active") & (F.col("credit_limit") >= 10700)))
)

country_group_summary = (
    filtered_customers
    .groupBy(
        F.when(F.col("country").isin(["DE", "SG"]), "core").otherwise("other").alias("country_group"),
        F.when(F.col("city").isin(["London", "New York"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.max("age").alias("max_age"),
    )
)

result = (
    country_group_summary
    .filter(F.col("credit_limit_count") > 5)
    .orderBy("country_group", "city_group")
    .limit(50)
)
