from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3]) | (F.col("product_id") == 6))
    .select(
        "id",
        "revenue",
        F.when(F.col("revenue") >= 3900, "High").when(F.col("revenue") >= 1000, "Medium").otherwise("Low").alias("revenue_tier"),
    )
)
