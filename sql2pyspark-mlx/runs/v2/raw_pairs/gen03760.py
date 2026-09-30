from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2024-06-01") & F.col("product_id").isin([1, 6, 9, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.date_sub("sale_date", 69).alias("sale_date_minus_69d"),
    )
)
