from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.supplier_name").like("T%") | (F.col("p.price") >= 1060))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("s.city"),
    )
)
