from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.countDistinct("age").alias("unique_segments"),
    )
    .filter(F.col("avg_credit_limit") > 53)
    .orderBy("unique_segments")
)
