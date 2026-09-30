from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
    .filter(F.col("cost_count") < 2)
)
