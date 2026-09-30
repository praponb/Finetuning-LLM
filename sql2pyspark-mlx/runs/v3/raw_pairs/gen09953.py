from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "Consumer"]))
    .groupBy(F.year("signup_date").alias("signup_year"), "is_active")
    .agg(
        F.countDistinct("age").alias("unique_citys"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("min_age") < 61)
    .orderBy(F.col("signup_year").desc(), "is_active")
    .limit(20)
)
