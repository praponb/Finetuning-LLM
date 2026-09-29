from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 4, 11]) | (F.col("units") > 36))
    .select(
        "product_id",
        "store_id",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
