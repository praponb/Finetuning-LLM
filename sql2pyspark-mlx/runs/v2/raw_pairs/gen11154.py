from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Sports", "Books"]))
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stddev_cost") >= 60)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("ss.stddev_cost"))
)
