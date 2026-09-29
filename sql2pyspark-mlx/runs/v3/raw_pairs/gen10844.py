from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 7, 8]))
    .select(
        "id",
        "carrier",
        "status",
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
)
