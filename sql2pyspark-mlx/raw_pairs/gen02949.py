from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.month("delivered_date").isin([5, 7, 8]) & (F.col("status") != "LOST") & F.col("delivered_date").between("2024-12-30", "2025-04-01"))
)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = status_summary.filter(F.col("stddev_cost") > 24)
