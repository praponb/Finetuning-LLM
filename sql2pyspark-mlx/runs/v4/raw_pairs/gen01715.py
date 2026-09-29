from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 1)
    .orderBy("cnt")
)
