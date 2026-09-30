from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
)
