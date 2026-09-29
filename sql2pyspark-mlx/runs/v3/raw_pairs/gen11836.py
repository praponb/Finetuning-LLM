from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select("last_restock")
    .intersect(df_stores.select("opened_date"))
    .orderBy("last_restock")
)
