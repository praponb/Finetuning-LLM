from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .select("sale_date")
    .union(df_inventory.select("last_restock"))
    .orderBy("sale_date")
)
