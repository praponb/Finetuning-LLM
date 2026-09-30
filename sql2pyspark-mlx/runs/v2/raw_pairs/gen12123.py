from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country", "segment")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("age")).otherwise(0)).alias("berlin_age"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("segment").desc(), F.col("stddev_credit_limit").desc())
)
