from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.round(F.col("s.rating"), 2).alias("rating_rounded"),
    )
)
