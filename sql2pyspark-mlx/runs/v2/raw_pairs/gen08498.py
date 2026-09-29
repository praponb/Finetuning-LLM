from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.category").isin(["Grocery", "Books", "Home"]))
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("revenue_range") > 64)
)
