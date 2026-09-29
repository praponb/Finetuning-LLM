from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.max("age").alias("max_age"))
)
