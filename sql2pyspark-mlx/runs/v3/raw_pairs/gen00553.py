from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("product_id").isin([1, 3, 6, 10]) & ((F.col("sale_date") <= "2024-09-01") | (F.col("store_id") == 3))))
    .select(
        "id",
        "revenue",
        "product_id",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
