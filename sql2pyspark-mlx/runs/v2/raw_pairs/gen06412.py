from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").like("Wa%"))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_inventory"),
    )
    .orderBy(F.col("brand").desc())
)
