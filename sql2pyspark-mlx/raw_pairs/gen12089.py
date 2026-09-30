from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") == 2024) & (F.col("revenue") <= 1400))
    .select(
        "id",
        "product_id",
        "store_id",
        F.round("revenue", 1).alias("revenue_rounded"),
    )
)
