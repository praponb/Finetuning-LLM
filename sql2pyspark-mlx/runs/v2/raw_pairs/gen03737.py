from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .select(
        "id",
        "product_id",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc())
)
