from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
