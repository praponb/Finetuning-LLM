from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .groupBy("carrier", "status")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 65)
)
