from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)
