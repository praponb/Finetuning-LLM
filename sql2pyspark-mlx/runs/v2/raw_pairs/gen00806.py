from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)
