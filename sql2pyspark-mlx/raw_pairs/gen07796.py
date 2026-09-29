from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3200, "High").when(F.col("revenue") >= 1300, "Medium").otherwise("Low").alias("revenue_tier"),
    )
)

result = (
    valid
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([7, 8]))
        .select("store_id")
    )
)
