from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("name").like("Pi%") & F.col("is_active")) | F.col("email").isNotNull())
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.min("signup_date").alias("min_signup_date"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .orderBy(F.col("total_credit_limit").desc())
)
