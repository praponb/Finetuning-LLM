from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.num_products") >= 4)
    .select(F.col("s.supplier_name"), F.col("t.min_cost"), F.col("t.num_products"))
    .orderBy("num_products")
)
