from pyspark.sql import functions as F

joined = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.category").isin(["Sports", "Books"]))
    .select(
        "s.id",
        "s.product_id",
        "s.units",
        "s.revenue",
        F.col("d.stock_qty"),
        F.col("d.category"),
    )
)

result = joined.select("id", "units", "revenue", "stock_qty")
