from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category") == "Sports")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.when(F.col("p.category") == "Books", 1).otherwise(0)).alias("books_count"),
    )
    .orderBy("books_count", "country")
    .limit(20)
)
