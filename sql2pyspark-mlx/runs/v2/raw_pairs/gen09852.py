from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2025-08-01") | F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "product_id",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
