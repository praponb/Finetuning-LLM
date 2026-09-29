from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~F.col("p.category").isin(["Electronics", "Books", "Sports"]))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
    )
)
