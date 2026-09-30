from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(F.col("age").between(43, 51) & F.col("email").like("%@yahoo.com") & F.col("age").isNotNull()))
    .agg(
        F.countDistinct("credit_limit").alias("unique_citys"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy("unique_citys", "last_signup_date")
)
