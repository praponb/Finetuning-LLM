from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "left")
    .filter(F.col("products_by_supplier.grocery_count") > 4)
    .select(
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
        F.col("products_by_supplier.initech_count"),
        F.col("products_by_supplier.grocery_count"),
    )
    .orderBy("supplier_name", F.col("initech_count").desc())
)
