from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 2200)
    .select(
        "product_id",
        "revenue",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)
