from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("quantity") > 133)

agg = (
    valid_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    agg
    .filter(F.col("max_quantity") >= 60)
    .select("last_restock_year", "max_quantity")
    .orderBy("max_quantity", "last_restock_year")
    .limit(50)
)
