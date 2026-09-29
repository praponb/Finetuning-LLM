from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "status",
        "carrier",
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)
