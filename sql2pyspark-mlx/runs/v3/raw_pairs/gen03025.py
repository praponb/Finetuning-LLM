from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-08-01", "2025-04-01"))
    .select(
        "store_id",
        "sale_date",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc())
)
