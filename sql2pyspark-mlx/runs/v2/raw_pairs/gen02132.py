from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-05-01", "2025-02-01") & F.col("sale_date").between("2024-02-01", "2025-04-01"))
    .select(
        "store_id",
        "sale_date",
        "product_id",
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)
