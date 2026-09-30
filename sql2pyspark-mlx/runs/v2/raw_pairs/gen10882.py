from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
    .orderBy(F.col("stddev_cost").desc())
)
