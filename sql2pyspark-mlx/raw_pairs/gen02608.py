from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 13300) & ~F.col("is_active") & (F.col("credit_limit") >= 7600))
    .groupBy("segment")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .filter(F.col("stddev_age") < 23)
)
