from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 6]))
    .select(
        "id",
        "revenue",
        "product_id",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)
