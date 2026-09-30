from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("category") == "Books")
    .groupBy("supplier_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.globex_count") >= 9)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("pbs.cost_count"),
        F.col("pbs.globex_count"),
    )
    .orderBy(F.col("cost_count").desc(), "id")
    .limit(10)
)
