from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 36) & (F.col("store_id") == 6))
    .select(
        "id",
        "sale_date",
        "product_id",
        "store_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
