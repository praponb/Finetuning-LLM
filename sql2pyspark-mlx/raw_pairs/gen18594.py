from pyspark.sql import functions as F

result = (
    df_shipments
    .groupingSets([["status", "order_id"], ["status"], []], "status", "order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.grouping("status").alias("grouping_level"),
    )
)
