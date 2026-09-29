from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-10-20", "2025-05-01"))
    .select(
        "id",
        "status",
        F.when(F.col("cost") >= 92, "High").when(F.col("cost") >= 23, "Medium").otherwise("Low").alias("cost_level"),
        F.col("cost").cast("int").alias("cost_int"),
    )
    .orderBy(F.col("cost_int").desc(), F.col("cost_level").desc())
)
