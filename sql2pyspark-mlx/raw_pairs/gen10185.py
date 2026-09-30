from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "units",
        "store_id",
        "product_id",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)
