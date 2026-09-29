from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost").between(360, 640))
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("s.country"), F.col("ss.category_values"))
)
