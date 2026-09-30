from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull())
    .select(
        "id",
        "order_id",
        "status",
        "shipped_date",
        F.when(F.col("cost") >= 86, "High").when(F.col("cost") >= 39, "Medium").otherwise("Low").alias("cost_level"),
    )
    .orderBy(F.col("shipped_date").desc(), "id")
)
