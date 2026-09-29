from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("category")
    .limit(1)
)
