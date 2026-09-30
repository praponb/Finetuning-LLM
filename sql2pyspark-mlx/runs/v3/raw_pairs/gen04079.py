from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 3500) | F.col("product_id").isin([4, 6]))
    .select(
        "id",
        "revenue",
        "product_id",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc(), "revenue")
)
