from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.country").isin(["US", "JP"]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.price"),
        F.col("s.country"),
        F.round(F.col("s.rating"), 1).alias("rating_rounded"),
    )
)
