from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "order_id",
        "status",
        F.when(F.col("cost") >= 102, "High").when(F.col("cost") >= 34, "Medium").otherwise("Low").alias("cost_band"),
    )
    .orderBy("status")
)
