from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2024-05-01") & F.col("product_id").isin([2, 3]) & (F.col("revenue") >= 3600))
    .select(
        "store_id",
        "product_id",
        F.round((F.col("revenue") * 1.1), 2).alias("adjusted_revenue"),
    )
)
