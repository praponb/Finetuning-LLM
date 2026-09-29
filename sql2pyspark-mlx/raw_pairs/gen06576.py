from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([23, 32]) | (F.col("cost") < 91))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)
