from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.avg_price") > 1060)
    .select(
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("t.brand_list"),
        F.col("t.avg_price"),
    )
    .orderBy("avg_price")
)
