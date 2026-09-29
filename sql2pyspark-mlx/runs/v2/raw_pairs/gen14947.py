from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "is_active")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
    )
    .filter(F.col("avg_credit_limit") >= 66)
    .orderBy(F.col("avg_credit_limit").desc())
)
