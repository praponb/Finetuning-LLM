from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 7700)
    .select(
        "name",
        "age",
        "country",
        F.date_sub("signup_date", 10).alias("signup_date_minus_10d"),
    )
)
