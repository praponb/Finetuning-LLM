from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 55)
    .groupBy("country")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .orderBy("country")
)
