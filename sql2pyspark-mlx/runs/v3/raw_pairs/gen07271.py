from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "product_id",
        F.col("units").cast("double").alias("units_num"),
        F.round("units", 2).alias("units_rounded"),
    )
)
