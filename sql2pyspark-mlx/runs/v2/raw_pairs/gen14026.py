from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)
