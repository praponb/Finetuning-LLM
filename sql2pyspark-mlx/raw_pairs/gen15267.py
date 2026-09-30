from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "order_id",
        "cost",
        F.when(F.col("cost") >= 83, "High").when(F.col("cost") >= 41, "Medium").otherwise("Low").alias("cost_level"),
    )
)
