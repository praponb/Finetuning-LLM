from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("s.rating") >= 3.6)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("city")
)
