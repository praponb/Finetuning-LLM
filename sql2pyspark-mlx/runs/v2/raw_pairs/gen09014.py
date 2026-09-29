from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty") >= 144)
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("t.category_values"),
        F.col("t.books_count"),
    )
    .orderBy(F.col("country").desc())
)
