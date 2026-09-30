from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2023-08-01") & (F.col("revenue") <= 1500) & F.month("sale_date").isin([2, 7, 12]))
    .select(
        "id",
        "store_id",
        "sale_date",
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)
