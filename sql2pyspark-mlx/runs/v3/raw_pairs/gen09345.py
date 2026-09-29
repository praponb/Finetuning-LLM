from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("s.city"),
        F.lower(F.col("s.city")).alias("city_lower"),
    )
    .orderBy(F.col("stock_qty").desc(), "city_lower")
)
