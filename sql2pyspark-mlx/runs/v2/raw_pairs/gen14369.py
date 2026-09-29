from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-01", "2024-09-01"))
    .select(
        "id",
        "product_id",
        "revenue",
        F.least("revenue", "units").alias("least_revenue_units"),
    )
    .orderBy(F.col("product_id").desc())
)
