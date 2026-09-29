from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.count(F.when(F.col("p.category") == "Books", 1)).alias("books_count"))
    .orderBy("city")
)
