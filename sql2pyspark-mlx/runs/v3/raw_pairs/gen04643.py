from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~((F.col("p.price") < 720) & (F.col("p.category") == "Fashion")))
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
)
