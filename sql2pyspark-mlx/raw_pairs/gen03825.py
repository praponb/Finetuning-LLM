from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units").between(17, 20) & F.col("store_id").isin([1, 4, 5, 6])) | (F.col("units") <= 37))
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        F.date_sub("sale_date", 60).alias("sale_date_minus_60d"),
    )
)
