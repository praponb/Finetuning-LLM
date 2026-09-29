from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "carrier",
        F.when(F.col("cost") >= 73, "High").when(F.col("cost") >= 31, "Medium").otherwise("Low").alias("cost_level"),
    )
)
