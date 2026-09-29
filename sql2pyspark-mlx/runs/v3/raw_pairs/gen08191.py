from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_categorys"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .filter(F.col("supplier_stats.unique_categorys") > 5)
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_stats.unique_categorys"),
        F.col("supplier_stats.num_products"),
    )
    .orderBy(F.col("city").desc(), F.col("num_products").desc())
)
