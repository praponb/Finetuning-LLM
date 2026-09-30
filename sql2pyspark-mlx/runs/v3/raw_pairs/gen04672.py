from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .union(df_inventory.select("last_restock"))
)
