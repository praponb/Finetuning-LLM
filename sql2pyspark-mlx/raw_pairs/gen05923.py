from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.sum("age").alias("total_age"),
        F.count("age").alias("age_count"),
    )
    .orderBy(F.col("stddev_credit_limit").desc(), "country")
)
