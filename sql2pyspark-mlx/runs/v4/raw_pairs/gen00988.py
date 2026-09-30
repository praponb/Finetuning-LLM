from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.bit_or("age").alias("age_bit_or_2"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
    )
    .filter(F.col("age_bit_or") < 1)
    .orderBy(F.col("country").asc_nulls_last())
)
