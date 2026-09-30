from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2023-09-18") | F.col("revenue").between(1200, 3300))
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("store_id").desc())
)
