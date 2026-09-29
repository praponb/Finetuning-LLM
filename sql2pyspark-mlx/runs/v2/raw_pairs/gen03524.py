from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "sale") & (F.col("p.cost") <= 390))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy("country")
)
