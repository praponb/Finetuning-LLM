from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-10-01")
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 2)
    .drop("total_count")
)
