from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("price").alias("max_price"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("supplier_summary.max_price"),
    )
    .orderBy(F.col("id").desc(), "rating")
    .limit(50)
)
