from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("city").isin("Singapore", "Bangkok")) & (F.upper(F.trim(F.col("wh_code"))).like("K%")))
    .select(F.col("id").alias("ref_id"), F.col("wh_code").alias("category"))
)

sku_rows = (
    df_skus
    .filter((F.col("weight_kg").between(4.7, 18.5)) & (F.array_contains(F.col("labels"), "fragile")))
    .select("id", "category")
)

combined = warehouse_side.union(sku_rows)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("category")
)
