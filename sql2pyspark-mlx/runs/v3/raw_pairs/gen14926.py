from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.brand") != "Initech") & (F.col("p.launch_date") > "2025-04-01"))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("city")
)
