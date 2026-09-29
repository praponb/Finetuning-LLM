from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 6, 7]) | F.col("sale_date").between("2025-01-01", "2025-04-18"))
    .select(
        "id",
        "revenue",
        "sale_date",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
