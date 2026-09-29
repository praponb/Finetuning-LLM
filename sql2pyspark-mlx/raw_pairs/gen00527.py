from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.upper("product_name").like("%APT%"))
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("num_products"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_cost") > 3900)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("t.num_products"),
        F.col("t.total_cost"),
    )
    .orderBy("country")
)
