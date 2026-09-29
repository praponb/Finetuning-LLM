from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.col("t.price_count"),
        F.col("t.electronics_price"),
    )
    .orderBy("supplier_name")
)
