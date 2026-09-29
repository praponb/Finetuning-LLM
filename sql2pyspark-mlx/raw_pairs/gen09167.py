from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .filter(F.col("i.store_id").isin([2, 5]))
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.count("*").alias("num_inventory"))
    .orderBy("num_inventory", "launch_quarter")
    .limit(5)
)
