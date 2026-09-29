from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.cost")).otherwise(0)).alias("books_cost"),
        F.count(F.col("p.stock_qty")).alias("stock_qty_count"),
    )
)
