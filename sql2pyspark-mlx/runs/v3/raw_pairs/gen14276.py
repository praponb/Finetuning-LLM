from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 14700)
    .groupBy("country", F.year("signup_date").alias("signup_year"))
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.countDistinct("age").alias("unique_segments"),
    )
    .filter(F.col("stddev_credit_limit") < 66)
    .select("country", "signup_year", "stddev_credit_limit", "unique_segments")
)
