from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 27)
    .orderBy("country")
    .limit(50)
)
