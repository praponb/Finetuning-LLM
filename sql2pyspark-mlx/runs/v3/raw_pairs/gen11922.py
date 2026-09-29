from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 27) & (F.col("store_id") == 3))
    .select(
        "id",
        "revenue",
        "sale_date",
        F.when(F.col("revenue") >= 4200, "High").when(F.col("revenue") >= 900, "Medium").otherwise("Low").alias("revenue_tier"),
    )
    .orderBy("revenue", "id")
    .limit(10)
)
