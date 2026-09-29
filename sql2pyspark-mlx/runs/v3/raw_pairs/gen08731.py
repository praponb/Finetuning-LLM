from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand").isin(["Acme", "Initech"]))
    .groupBy(F.col("p.category"))
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .orderBy("category")
)
