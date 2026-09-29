from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost").between(330, 630))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("cost")).otherwise(0)).alias("books_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.books_cost") > 75)
    .select(F.col("s.id"), F.col("s.country"), F.col("pbs.books_cost"))
    .orderBy(F.col("id").desc())
)
