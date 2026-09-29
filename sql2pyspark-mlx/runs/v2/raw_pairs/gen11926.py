from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "stock_qty",
        "cost",
        "price",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.sort_array("tags").alias("sorted_tags"),
    )
    .orderBy(F.col("price_filled").desc(), "price")
)
