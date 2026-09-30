from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-08-01")
    .groupBy("carrier", "status")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("avg_cost")
)
