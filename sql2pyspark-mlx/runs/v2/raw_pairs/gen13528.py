from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com") & (F.col("age") <= 67) & F.upper("name").like("%VY %"))
    .groupBy(F.month("signup_date").alias("signup_month"), "segment")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
)
