from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("price") <= 1090) & (F.col("category") == "Books")) | ~F.col("category").isin(["Home", "Books", "Fashion"]))
    .select(
        "brand",
        "category",
        (F.col("cost") - F.col("stock_qty")).alias("cost_minus_stock_qty"),
    )
    .orderBy(F.col("category").desc())
)
