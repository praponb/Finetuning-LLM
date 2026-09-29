from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("t.total_count"),
        F.col("t.min_cost"),
    )
    .orderBy(F.col("total_count").desc(), "id")
    .limit(1)
)
