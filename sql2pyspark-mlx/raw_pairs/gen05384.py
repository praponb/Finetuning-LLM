from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 1) & F.col("product_id").isin([2, 5, 8, 10]))
    .select(
        "id",
        "sale_date",
        "store_id",
        "units",
        F.round((F.col("revenue") * 100), 2).alias("revenue_scaled"),
    )
)
