from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.countDistinct("credit_limit").alias("unique_segments"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.sum(F.when(F.col("country") == "JP", F.col("age")).otherwise(0)).alias("jp_age"),
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
    )
    .filter(F.col("consumer_count") > 30)
    .drop("consumer_count")
    .orderBy(F.col("avg_credit_limit").desc(), "jp_age")
)
