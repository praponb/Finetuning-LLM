from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "store_id",
        "product_id",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
