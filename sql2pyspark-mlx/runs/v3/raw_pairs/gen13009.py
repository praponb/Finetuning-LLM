from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2025-03-01") & (F.col("revenue") <= 3000))
    .select(
        "id",
        "product_id",
        "sale_date",
        "store_id",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
    .orderBy("store_id", "id")
    .limit(1)
)
