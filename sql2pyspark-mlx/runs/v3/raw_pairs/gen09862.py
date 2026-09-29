from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") > 13) & (F.col("product_id").isin([2, 3, 6, 10]) | (F.col("sale_date") > "2023-03-30")))
    .select(
        "id",
        "store_id",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
