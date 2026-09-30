from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "cost",
        "launch_date",
        "brand",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.concat_ws(",", "category", "brand").alias("category_brand_key"),
    )
)
