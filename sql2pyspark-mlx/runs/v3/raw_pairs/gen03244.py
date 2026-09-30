from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 2600) & F.col("store_id").isin([2, 3, 4, 5]) & F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "units",
        "product_id",
        "store_id",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(20)
)
