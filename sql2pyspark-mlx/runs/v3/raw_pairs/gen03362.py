from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("price").alias("avg_price"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("t.avg_price"),
        F.col("t.min_cost"),
    )
    .orderBy("supplier_name")
)
