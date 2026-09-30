from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        F.when(F.col("cost") >= 83, "High").when(F.col("cost") >= 25, "Medium").otherwise("Low").alias("cost_level"),
    )
)
