from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("product_name").like("%Nois%"))
    .groupBy("supplier_id")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.count("price").alias("price_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.max_stock_qty") >= 370)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("t.max_stock_qty"),
        F.col("t.price_count"),
    )
    .orderBy("supplier_name")
)
