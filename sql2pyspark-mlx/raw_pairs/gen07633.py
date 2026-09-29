from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Berlin")
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("avg_credit_limit") > 79)
    .select("signup_year", "avg_credit_limit", "min_age")
)
