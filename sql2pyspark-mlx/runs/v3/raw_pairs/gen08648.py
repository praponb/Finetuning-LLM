from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        "brand",
        "category",
        F.round("price", 2).alias("price_rounded"),
    )
    .orderBy("category", "id")
    .limit(10)
)
