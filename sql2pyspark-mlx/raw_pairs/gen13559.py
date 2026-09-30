from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty") == 248)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .filter(F.col("S.home_count") >= 45)
    .select(F.col("s.rating"), F.col("S.home_count"), F.col("S.brand_values"))
    .orderBy("rating")
)
