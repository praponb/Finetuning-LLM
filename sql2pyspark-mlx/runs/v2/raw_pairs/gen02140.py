from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(25, 35))
    .select(
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)
