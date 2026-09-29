from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"))
    .filter(F.col("ups_count") < 4)
)
