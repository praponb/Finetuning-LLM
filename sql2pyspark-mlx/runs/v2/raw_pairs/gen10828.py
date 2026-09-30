from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2025-07-01") & (F.col("delivered_date") < "2023-10-01"))
    .groupBy("carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
    )
)
