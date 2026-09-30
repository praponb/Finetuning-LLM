from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("age") < 31) & ~F.col("email").like("%@gmail.com") & F.col("email").like("%@gmail.com"))
    .withColumn("segment_prefix", F.substring("segment", 1, 3))
)

summary = (
    src
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "segment",
    )
    .agg(F.max("signup_date").alias("last_signup_date"))
)

result = (
    summary
    .select("signup_cohort", "segment", "last_signup_date")
    .orderBy("signup_cohort", "segment")
    .limit(20)
)
