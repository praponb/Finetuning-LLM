from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["JP", "UK"]))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .filter(F.col("age_range") < 39)
    .orderBy("age_range", "signup_month")
)
