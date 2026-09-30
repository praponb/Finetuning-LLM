from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.min(F.col("p.stock_qty")).alias("min_stock_qty"),
    )
    .filter(F.col("product_count") >= 5)
    .orderBy("id", "supplier_name")
    .limit(5)
)
