from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 67)
    .groupBy("country")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)
