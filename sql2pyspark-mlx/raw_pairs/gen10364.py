from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", "city")
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.max("age").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
