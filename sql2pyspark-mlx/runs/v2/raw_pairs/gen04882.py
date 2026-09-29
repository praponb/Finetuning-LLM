from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Flash") | (F.col("cost") < 75))
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
    .orderBy("status")
)
