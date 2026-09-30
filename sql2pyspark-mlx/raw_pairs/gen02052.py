from pyspark.sql import functions as F

src = df_customers.filter(F.col("age") < 39)

customers_stats = (
    src
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("credit_limit")).otherwise(0)).alias("singapore_credit_limit"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
)

result = (
    customers_stats
    .filter(F.col("consumer_count") >= 9)
    .orderBy(F.col("signup_year").desc())
)
