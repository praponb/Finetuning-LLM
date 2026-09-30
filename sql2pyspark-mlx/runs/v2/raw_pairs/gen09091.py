from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("S%") & F.array_contains("tags", "premium"))
    .select(
        "product_name",
        F.greatest("cost", "stock_qty").alias("greatest_cost_stock_qty"),
        F.col("stock_qty").cast("double").alias("stock_qty_str"),
    )
    .orderBy(F.col("product_name").desc())
)
