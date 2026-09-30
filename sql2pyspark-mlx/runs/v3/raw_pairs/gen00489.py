from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.coalesce(F.col("ss.revenue"), F.lit(0)).alias("revenue_filled"),
    )
    .orderBy("store_id")
)
