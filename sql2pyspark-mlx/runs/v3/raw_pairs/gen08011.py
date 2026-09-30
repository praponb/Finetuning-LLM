from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("rating") >= 3.5).select("id")

result = (
    df_products.alias("p")
    .join(excluded_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .filter(F.col("p.product_name").like("De%"))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.product_name"))
    .orderBy("supplier_id", "id")
    .limit(5)
)
