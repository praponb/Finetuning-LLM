from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
    )
    .orderBy("launch_quarter")
    .limit(5)
)
