from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Berlin", "Singapore", "New York"]))
    .groupBy("segment")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") >= 48)
    .select("segment", "credit_limit_count", "max_age")
    .orderBy("segment")
    .limit(20)
)
