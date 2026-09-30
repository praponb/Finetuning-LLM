from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3300) & (F.col("sale_date") >= "2025-08-30") & (F.col("sale_date") <= "2024-10-01"))
    .select(
        "revenue",
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)
