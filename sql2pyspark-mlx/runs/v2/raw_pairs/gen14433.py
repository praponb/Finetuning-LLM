from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") <= 2100) & (F.col("revenue") > 4000)) | F.col("product_id").isin([7, 9]))
    .select(
        "id",
        "product_id",
        "revenue",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
