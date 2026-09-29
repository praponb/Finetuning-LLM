from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 38) | F.col("sale_date").between("2023-07-05", "2024-01-09"))
    .select(
        "id",
        "sale_date",
        "store_id",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)
