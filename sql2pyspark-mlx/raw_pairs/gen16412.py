from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .select(
        "revenue",
        "units",
        "product_id",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
    .orderBy(F.col("units").desc())
)
