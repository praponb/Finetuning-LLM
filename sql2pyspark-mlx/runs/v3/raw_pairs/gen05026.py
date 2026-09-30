from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-06-19", "2024-09-25") | F.col("product_id").isin([4, 5, 8]))
    .select(
        "revenue",
        "store_id",
        "units",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)
