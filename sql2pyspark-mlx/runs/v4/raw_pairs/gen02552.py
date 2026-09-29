from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc())
)
