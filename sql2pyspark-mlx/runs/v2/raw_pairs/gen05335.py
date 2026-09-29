from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .orderBy(F.col("num_inventory").desc())
)
