from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("s.country").isin(["SG", "JP"]))
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.category")))).alias("category_values"),
    )
    .orderBy("category_values", "id", "supplier_name")
    .limit(5)
)
