from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(12, 34))
    .select(
        "id",
        "product_id",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)
