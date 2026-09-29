from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
    .filter(F.col("in_transit_cost") >= 47)
)
