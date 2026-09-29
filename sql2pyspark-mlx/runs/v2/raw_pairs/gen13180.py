from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 3500)
    .select(
        "id",
        "units",
        "product_id",
        "revenue",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)
