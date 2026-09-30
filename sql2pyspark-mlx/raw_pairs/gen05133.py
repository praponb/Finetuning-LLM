from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.units_count") > 3)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("ps.unique_stores"),
        F.col("ps.units_count"),
    )
)
