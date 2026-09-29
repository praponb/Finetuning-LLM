from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "Kerry", "Flash"]))
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
        F.sum("cost").alias("total_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("lost_cost") < 45)
)
