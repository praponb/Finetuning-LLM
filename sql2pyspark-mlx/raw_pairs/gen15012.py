from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"))
    .orderBy("flash_count")
)
