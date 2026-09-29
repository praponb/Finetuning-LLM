from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy("carrier", "status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
    .orderBy("status", "carrier")
)
