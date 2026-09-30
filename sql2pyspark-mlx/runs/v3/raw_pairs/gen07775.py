from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "JP")
    .groupBy("city")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("stddev_age") >= 37)
)
