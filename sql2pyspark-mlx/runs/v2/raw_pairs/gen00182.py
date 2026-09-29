from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .filter(F.col("age_range") > 54)
)
