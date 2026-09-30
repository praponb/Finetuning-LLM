from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["TH", "SG"]))
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_products"))
    .orderBy(F.col("unique_products").desc())
)
