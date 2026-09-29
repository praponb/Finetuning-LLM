from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.price")).otherwise(0)).alias("books_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.category")))).alias("category_values"),
    )
    .orderBy(F.col("books_price").desc())
)
