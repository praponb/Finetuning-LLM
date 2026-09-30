from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7]) & F.col("store_id").isin([1, 4, 6]))
    .select(
        "id",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
