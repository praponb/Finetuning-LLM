from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Stark"]))
    .groupBy("supplier_id")
    .agg(F.sum("price").alias("total_price"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.col("t.total_price"),
    )
    .orderBy("total_price", "id")
)
