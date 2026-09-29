from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Jack%"))
    .groupBy("city")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .orderBy("avg_credit_limit", "city")
)
