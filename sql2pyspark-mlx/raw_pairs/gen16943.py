from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") >= 75)
)
