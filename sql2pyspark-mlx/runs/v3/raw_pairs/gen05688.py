from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("signup_date") < "2024-02-24") & F.col("signup_date").between("2023-07-21", "2025-01-19"))
    .withColumn("age_adjusted", F.round((F.col("age") * 1.1), 2))
)

agg = (
    src
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.round(F.avg("age_adjusted"), 2).alias("avg_age_adjusted"),
        F.min("age").alias("min_age"),
    )
)

result = (
    agg
    .orderBy(F.col("min_age").desc(), "signup_cohort")
    .limit(5)
)
