from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
)
