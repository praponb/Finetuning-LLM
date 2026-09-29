from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 6]) | F.col("units").between(23, 24))
    .select(
        "store_id",
        "units",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
