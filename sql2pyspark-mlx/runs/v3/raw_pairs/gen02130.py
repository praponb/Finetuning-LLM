from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & F.col("carrier").isNull())
    .groupBy("status", "carrier")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("cost").alias("min_cost"),
    )
)
