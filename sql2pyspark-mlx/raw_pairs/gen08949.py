from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        F.floor("units").alias("units_whole"),
    )
)
