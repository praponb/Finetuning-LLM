from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .filter(F.col("P.acme_count") >= 71)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("P.acme_count"),
        F.col("P.max_stock_qty"),
    )
    .orderBy(F.col("max_stock_qty").desc())
)
