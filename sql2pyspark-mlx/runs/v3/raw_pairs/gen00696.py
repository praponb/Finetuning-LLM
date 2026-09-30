from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("avg_credit_limit").desc())
)
