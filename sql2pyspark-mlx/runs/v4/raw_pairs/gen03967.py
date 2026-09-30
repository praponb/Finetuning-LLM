from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id").isin([5, 6, 8, 9]))

agg = (
    recent_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = agg.select("last_restock_year", "unique_stores")
