from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        F.round((F.col("cost") * 1.1), 2).alias("cost_scaled"),
        F.upper("status").alias("status_upper"),
    )
    .orderBy("cost_scaled", "id")
    .limit(5)
)
