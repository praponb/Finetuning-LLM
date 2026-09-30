from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.brand"),
        F.col("s.city"),
        F.coalesce(F.col("s.rating"), F.lit(0)).alias("rating_filled"),
    )
    .orderBy(F.col("units").desc())
)
