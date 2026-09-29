from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"))
    .filter(F.col("flash_count") > 2)
)
