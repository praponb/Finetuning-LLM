from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("p.brand") == "Initech") & F.col("s.supplier_name").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.city"),
    )
    .orderBy("city", "id")
    .limit(50)
)
