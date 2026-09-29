from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "SG")
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") > 75)
    .orderBy(F.col("signup_year").desc())
)
