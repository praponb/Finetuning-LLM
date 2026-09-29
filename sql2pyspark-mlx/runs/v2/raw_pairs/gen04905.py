from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("max_credit_limit") > 6300)
    .orderBy("max_credit_limit")
)
