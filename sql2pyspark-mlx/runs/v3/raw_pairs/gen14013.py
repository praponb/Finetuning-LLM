from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .groupBy("supplier_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("S.max_cost"),
        F.col("S.brand_values"),
    )
    .orderBy(F.col("rating").desc(), "id")
    .limit(5)
)
