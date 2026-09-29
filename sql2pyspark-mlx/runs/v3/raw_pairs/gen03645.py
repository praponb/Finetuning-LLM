from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("price") <= 300)
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.countDistinct("cost").alias("unique_brands"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .filter(F.col("supplier_stats.unique_brands") > 6)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
        F.col("supplier_stats.total_cost"),
        F.col("supplier_stats.unique_brands"),
    )
    .orderBy(F.col("total_cost").desc())
)
