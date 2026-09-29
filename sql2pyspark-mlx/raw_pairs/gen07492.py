from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 6]))
    .select("quantity")
    .subtract(df_inventory.filter(F.year("last_restock") >= 2023).select("quantity"))
)
