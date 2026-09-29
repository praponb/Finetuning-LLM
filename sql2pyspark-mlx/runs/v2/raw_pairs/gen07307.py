from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([13, 21, 31, 38]) & F.col("category").isin(["Books", "Grocery", "Electronics"]))
    .select(
        "brand",
        "product_name",
        "cost",
        F.round((F.col("price") * 0.9), 2).alias("adjusted_price"),
        F.size("tags").alias("tags_count"),
    )
    .orderBy("product_name", F.col("cost").desc())
)
