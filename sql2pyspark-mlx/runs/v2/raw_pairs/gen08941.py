from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.unique_suppliers") > 4)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("ss.books_count"),
        F.col("ss.unique_suppliers"),
    )
)
