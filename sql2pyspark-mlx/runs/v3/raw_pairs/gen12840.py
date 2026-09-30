from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([25, 32, 35, 39]))
    .groupBy("status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
    .filter(F.col("max_cost") < 52)
)
