from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id").isin([2, 6, 8]) & F.col("revenue").between(1200, 1500)) | (F.col("sale_date") <= "2025-10-31"))
    .select(
        "id",
        "store_id",
        F.date_trunc("year", "sale_date").alias("sale_date_period"),
        F.month("sale_date").alias("sale_date_month"),
    )
)
