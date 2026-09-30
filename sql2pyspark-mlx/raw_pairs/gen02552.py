from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .orderBy("stddev_age")
)
