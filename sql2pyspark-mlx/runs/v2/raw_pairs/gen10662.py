from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("quantity") != 107)

last_restock_year_summary = (
    filtered_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = last_restock_year_summary.filter(F.col("avg_quantity") < 80)
