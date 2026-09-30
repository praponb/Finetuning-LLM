from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("product_id").isin([1, 4, 6, 9]) & (F.year("last_restock") >= 2024)) | (F.col("store_id") == 3))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

last_restock_quarter_summary = (
    src
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.min("quantity").alias("min_quantity"))
)

result = last_restock_quarter_summary.filter(F.col("min_quantity") > 160)
