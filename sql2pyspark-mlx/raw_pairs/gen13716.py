from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("credit_limit").between(10000, 14700) & F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.ceil("credit_limit").alias("credit_limit_whole"),
        F.length("email").alias("email_length"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .filter(F.col("credit_limit_stddev") > 0)
    .orderBy(F.col("age_stddev").desc_nulls_last())
)
