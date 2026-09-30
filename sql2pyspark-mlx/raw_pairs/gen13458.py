from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") != "US") | (F.year("signup_date") == 2025))
    .groupBy(
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 43, "mid").otherwise("low").alias("age_bucket"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(
        F.countDistinct("age").alias("unique_citys"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") < 50)
)
