from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(F.min("age").alias("min_age"))
    .filter(F.col("min_age") < 44)
    .select("segment", "min_age")
    .orderBy("min_age", "segment")
    .limit(50)
)
