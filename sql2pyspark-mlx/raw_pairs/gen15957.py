from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("cost")).otherwise(0)).alias("umbrella_cost"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("t.umbrella_cost"),
        F.col("t.avg_price"),
    )
)
