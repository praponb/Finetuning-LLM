from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.units") <= 38)
    .select(
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("p.price"),
    )
    .orderBy("product_name")
)
