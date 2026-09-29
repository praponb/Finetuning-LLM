from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy(
        F.when(F.col("cost") >= 76, "high").when(F.col("cost") >= 19, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 5)
    .drop("row_count")
)
