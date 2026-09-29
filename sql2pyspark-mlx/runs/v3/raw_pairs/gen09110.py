from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.min("signup_date").alias("first_signup_date"),
    )
    .orderBy(F.col("first_signup_date").desc())
)
