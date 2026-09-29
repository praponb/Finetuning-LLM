from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_price") > 6200)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("t.total_price"),
        F.col("t.latest_launch_date"),
    )
)
