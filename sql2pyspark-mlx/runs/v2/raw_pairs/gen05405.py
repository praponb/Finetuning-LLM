from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.supplier_id").isin([15, 18, 20, 32]) & (F.col("p.stock_qty") <= 369))
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.initcap(F.trim(F.col("s.supplier_name"))).alias("supplier_name_title"),
    )
)
