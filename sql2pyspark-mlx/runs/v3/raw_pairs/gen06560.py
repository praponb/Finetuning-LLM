from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.avg_cost") > 350)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("pbs.category_list"),
        F.col("pbs.avg_cost"),
    )
)
