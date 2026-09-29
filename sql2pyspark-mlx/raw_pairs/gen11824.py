from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%USB%") & F.col("product_name").like("Sm%") & F.array_contains("tags", "premium"))
    .select(
        "id",
        "brand",
        "supplier_id",
        "stock_qty",
        F.date_trunc("week", "launch_date").alias("launch_date_period"),
    )
    .orderBy(F.col("supplier_id").desc())
)
