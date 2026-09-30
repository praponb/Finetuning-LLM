from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.supplier_id"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_products"))
    .orderBy(F.col("unique_products").desc())
)
