from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 1300) & (F.col("product_id") == 3))
    .select(
        "id",
        "product_id",
        "revenue",
        "units",
        F.year("sale_date").alias("sale_date_year"),
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy("revenue_per_unit")
)
