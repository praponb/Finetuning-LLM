from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 4)
    .select("sale_date")
    .intersect(df_inventory.select("last_restock"))
)
