from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-05-26", "2025-08-24") | (F.col("revenue") <= 1800))
    .select(
        "id",
        "product_id",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit_2"),
    )
    .orderBy(F.col("revenue_per_unit").desc())
)
