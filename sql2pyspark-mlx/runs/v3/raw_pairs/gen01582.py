from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .intersect(df_inventory.select("last_restock"))
)
