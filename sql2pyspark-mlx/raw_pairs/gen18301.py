from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 75)
    .select("last_restock")
    .unionByName(df_inventory.filter(F.col("quantity") != 111).select("last_restock"))
    .distinct()
    .orderBy("last_restock")
)
