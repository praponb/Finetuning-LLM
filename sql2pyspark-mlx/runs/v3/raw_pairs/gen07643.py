from pyspark.sql import functions as F

result = (
    df_customers
    .filter((~F.col("city").isin(["London", "Chiang Mai"]) & F.col("is_active")) | (F.col("signup_date") < "2025-03-01"))
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.countDistinct("age").alias("unique_countrys"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .orderBy(F.col("avg_age").desc())
)
