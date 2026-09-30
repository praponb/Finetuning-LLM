from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 1) & F.col("sale_date").between("2024-09-01", "2024-11-18"))
    .select(
        "id",
        "revenue",
        F.when(F.col("units") >= 44, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_band"),
    )
    .orderBy(F.col("units_band").desc(), "revenue")
)
