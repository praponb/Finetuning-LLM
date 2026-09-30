from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Globex"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.length("product_name").alias("product_name_length"),
    )
)

result = (
    filtered
    .select("id", "category")
    .intersect(
        df_products
        .filter(F.col("launch_date") <= "2023-05-01")
        .select("id", "category")
    )
)
