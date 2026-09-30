from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy("carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy(F.col("cost_range").desc())
)
