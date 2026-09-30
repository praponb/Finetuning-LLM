from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNull() & F.col("segment").isin(["Consumer", "SMB"]) & F.col("name").like("Oli%"))
    .groupBy("country", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
    )
)
