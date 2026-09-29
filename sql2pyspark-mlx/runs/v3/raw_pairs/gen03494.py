from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4]))
    .select(
        "id",
        "units",
        "product_id",
        F.col("revenue").cast("int").alias("revenue_int"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
