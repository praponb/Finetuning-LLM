from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .filter(F.col("returned_count") >= 42)
    .orderBy("returned_count")
)
