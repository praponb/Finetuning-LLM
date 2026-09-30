from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)
