from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("country").isin(["US", "DE"]) & F.col("segment").isNotNull())
    .withColumn("country_prefix", F.substring("country", 1, 2))
)

agg = (
    recent_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.count("age").alias("age_count"))
)

result = agg.orderBy(F.col("age_count").desc())
