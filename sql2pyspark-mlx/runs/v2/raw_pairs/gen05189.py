from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3900)
    .select(
        "id",
        "revenue",
        F.when(F.col("units") >= 38, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_tier"),
    )
    .orderBy("revenue", "id")
    .limit(3)
)
