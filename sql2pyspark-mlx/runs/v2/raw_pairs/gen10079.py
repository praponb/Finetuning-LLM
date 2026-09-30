from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-01-01", "2024-09-11") & F.col("sale_date").between("2023-06-07", "2024-06-22"))
    .select(
        "id",
        "units",
        F.round((F.col("revenue") * 0.9), 2).alias("adjusted_revenue"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
