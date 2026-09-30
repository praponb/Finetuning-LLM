from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 195)
    .select("product_id")
    .unionByName(df_inventory.filter(F.col("product_id") == 9).select("product_id"))
    .distinct()
)
