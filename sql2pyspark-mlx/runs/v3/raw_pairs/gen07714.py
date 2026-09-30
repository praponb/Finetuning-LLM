from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Books", 1).otherwise(0))) / F.count("*")), 1).alias("pct_books"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("ss.pct_books"),
        F.col("ss.stock_qty_count"),
    )
    .orderBy(F.col("city").desc(), "pct_books")
)
