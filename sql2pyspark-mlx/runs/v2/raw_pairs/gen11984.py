from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 27) & (F.col("revenue") <= 2600) & F.col("store_id").isin([1, 2, 4, 6]))
    .select(
        "store_id",
        "product_id",
        F.add_months("sale_date", 1).alias("sale_date_plus_months"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
