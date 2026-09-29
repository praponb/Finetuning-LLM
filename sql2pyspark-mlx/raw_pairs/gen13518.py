from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.books_count") > 4)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("ss.home_count"),
        F.col("ss.books_count"),
    )
    .orderBy(F.col("id").desc(), "home_count")
)
