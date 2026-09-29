from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
