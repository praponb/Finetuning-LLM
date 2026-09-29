from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("s.supplier_name"),
    )
    .orderBy("brand")
)
