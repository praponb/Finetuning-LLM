from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy("de_count")
)
