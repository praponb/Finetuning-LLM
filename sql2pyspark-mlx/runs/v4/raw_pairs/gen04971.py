from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 5, 7, 8]) & (F.col("store_id") == 5))
    .select(
        "id",
        "sale_date",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
    .orderBy(F.col("revenue").desc())
)
