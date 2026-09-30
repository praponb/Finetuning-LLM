from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "UPS")
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
    .filter(F.col("flash_count") < 62)
    .select("status", "flash_count")
)
