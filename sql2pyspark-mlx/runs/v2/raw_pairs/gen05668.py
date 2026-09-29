from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3800)
    .select(
        "id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)
