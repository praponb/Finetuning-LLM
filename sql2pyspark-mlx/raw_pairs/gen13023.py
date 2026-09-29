from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .orderBy(F.col("segment").desc())
)
