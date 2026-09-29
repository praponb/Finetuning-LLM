from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .groupBy("country")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .filter(F.col("stddev_age") > 44)
    .select("country", "stddev_age", "age_range")
    .orderBy("country")
)
