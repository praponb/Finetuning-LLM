from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "units",
        "revenue",
        "store_id",
        F.col("units").cast("string").alias("units_num"),
    )
)
