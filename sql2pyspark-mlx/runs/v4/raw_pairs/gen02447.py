from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("shipped_date").alias("latest_shipped_date"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy("latest_shipped_date")
)
