from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5, 10]) | (F.year("sale_date") >= 2024))
    .select(
        "units",
        "product_id",
        "store_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit_2"),
    )
)
