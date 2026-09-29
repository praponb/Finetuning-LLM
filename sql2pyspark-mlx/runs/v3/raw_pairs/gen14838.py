from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id").isin([1, 6]) | (F.col("quantity") == 139))

last_restock_year_summary = (
    filtered_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.max("quantity").alias("max_quantity"))
)

result = last_restock_year_summary.orderBy("last_restock_year")
