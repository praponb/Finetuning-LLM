from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)
