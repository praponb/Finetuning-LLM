from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("*").alias("product_count"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(F.col("suppliers.id"), F.col("suppliers.country"), F.col("t.product_count"))
    .orderBy("country", "id")
)
