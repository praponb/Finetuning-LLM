from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "launch_date",
        "cost",
        F.concat("product_name", F.lit(" "), "brand").alias("product_name_brand"),
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)
