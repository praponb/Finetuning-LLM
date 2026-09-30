from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 28) | F.col("product_id").isin([7, 10]))
    .select(
        "id",
        "sale_date",
        "store_id",
        "product_id",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)
