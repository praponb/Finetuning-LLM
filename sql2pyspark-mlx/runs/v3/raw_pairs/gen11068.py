from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(37, 39) & (F.col("sale_date") <= "2025-05-01"))
    .select(
        "id",
        "sale_date",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), "id")
)
