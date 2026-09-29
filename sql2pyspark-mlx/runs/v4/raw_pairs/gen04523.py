from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("p.category"), F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("category").desc(), "launch_year")
    .limit(10)
)
