from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
        F.avg("age").alias("avg_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("th_count") < 27)
    .orderBy(F.col("avg_credit_limit").desc())
)
