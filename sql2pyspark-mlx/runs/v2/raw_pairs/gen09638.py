from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-07-03")
    .select(
        "id",
        "revenue",
        "product_id",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)
