from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.rating"),
        F.initcap(F.trim(F.col("s.city"))).alias("city_title"),
    )
    .orderBy(F.col("stock_qty").desc())
)
