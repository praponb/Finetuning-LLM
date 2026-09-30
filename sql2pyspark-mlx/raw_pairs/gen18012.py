from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 11300) & ~F.col("email").like("%@startup.io"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("avg_credit_limit").desc())
)
