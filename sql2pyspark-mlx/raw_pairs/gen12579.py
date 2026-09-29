from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-06-10", "2024-07-01") & (F.col("delivered_date") <= "2024-12-24") & (F.col("shipped_date") < "2024-08-22"))
    .groupBy("status", "carrier", F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("cost").alias("cost_count"),
        F.count("*").alias("num_shipments"),
    )
    .filter(F.col("stddev_cost") >= 34)
)
