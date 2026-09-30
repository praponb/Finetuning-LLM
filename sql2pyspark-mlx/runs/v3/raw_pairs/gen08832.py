from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY").select("id")

result = (
    df_products.alias("products")
    .join(excluded_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_anti")
    .filter(F.col("products.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .select(F.col("products.id"), F.col("products.category"), F.col("products.cost"))
    .orderBy(F.col("id").desc())
    .limit(1)
)
