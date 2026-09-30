from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.lower("supplier_name").like("%ion%")).select("id")

result = (
    df_products.alias("products")
    .join(excluded_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_anti")
    .filter(F.col("products.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .filter(F.col("products.supplier_id") == 5)
    .select(F.col("products.id"), F.col("products.brand"), F.col("products.category"))
    .orderBy("id", F.col("category").desc())
)
