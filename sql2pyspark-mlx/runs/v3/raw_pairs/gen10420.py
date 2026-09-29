from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5, 6, 8]) | F.col("units").isNull())
    .select(
        "id",
        "store_id",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
    .orderBy(F.col("store_id").desc())
)
