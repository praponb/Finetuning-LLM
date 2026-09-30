from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "units",
        "product_id",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("units_filled").desc())
)
