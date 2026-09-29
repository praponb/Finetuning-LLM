from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("age") == 51)

customers_stats = (
    valid_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)

result = (
    customers_stats
    .filter(F.col("stddev_age") >= 63)
    .orderBy(F.col("stddev_age").desc())
)
