from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2023-04-05") & F.col("product_id").isin([3, 5, 8, 9]))
    .select(
        "store_id",
        F.col("units").cast("string").alias("units_num"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)
