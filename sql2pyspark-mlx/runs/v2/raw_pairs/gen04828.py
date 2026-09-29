from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "tags",
        "stock_qty",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
    .orderBy("launch_date_month_end")
)
