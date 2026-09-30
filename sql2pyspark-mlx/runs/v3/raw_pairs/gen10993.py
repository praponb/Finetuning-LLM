from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 6]) & F.col("product_id").isin([1, 7]))
    .select(
        "revenue",
        "product_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
        F.dayofmonth("sale_date").alias("sale_date_day"),
    )
)
