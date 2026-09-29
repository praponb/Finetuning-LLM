from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4]) & (F.col("units") <= 33))
    .select(
        "product_id",
        "revenue",
        "units",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)
