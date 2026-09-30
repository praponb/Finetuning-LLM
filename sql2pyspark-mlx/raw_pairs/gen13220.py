from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "country")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 2)
    .orderBy("cnt")
)
