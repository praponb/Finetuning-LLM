from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2800, 3300) & (F.col("sale_date") > "2024-01-01"))
    .select(
        "id",
        "revenue",
        "units",
        "product_id",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)
