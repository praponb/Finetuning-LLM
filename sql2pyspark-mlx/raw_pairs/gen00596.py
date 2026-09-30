from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 29) | (F.size("tags") >= 2))
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)
