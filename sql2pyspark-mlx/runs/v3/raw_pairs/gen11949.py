from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .groupBy("carrier")
    .agg(F.count("cost").alias("cost_count"))
)
