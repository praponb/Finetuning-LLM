from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "Corporate"]))
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)
