from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "TH")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("city", "avg_quantity")
    .limit(5)
)
