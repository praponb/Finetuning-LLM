from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.array_join("tags", ", ").alias("tags_text"),
        F.size("tags").alias("tags_count"),
    )
)

result = (
    filtered
    .select("brand")
    .subtract(
        df_products
        .filter(F.col("cost").between(280, 580))
        .select("brand")
    )
)
