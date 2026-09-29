from pyspark.sql import functions as F

enriched = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.price"), F.col("b.stock_qty"), F.col("b.category"))
)

result = (
    enriched
    .select("id", "store_id", "product_id", "units", "revenue")
    .orderBy("product_id")
)
