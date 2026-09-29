from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.total_cost") > 3500)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("t.total_price"),
        F.col("t.total_cost"),
    )
    .orderBy("country")
)
