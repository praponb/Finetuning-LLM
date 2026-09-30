from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 6]) & F.col("product_id").isin([6, 7, 9, 10]))
    .select(
        "id",
        "product_id",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
    )
)
