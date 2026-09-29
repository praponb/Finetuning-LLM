from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-08-01", "2023-09-18"))
    .select(
        "id",
        "units",
        "product_id",
        "sale_date",
        F.round("revenue", 1).alias("revenue_rounded"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
