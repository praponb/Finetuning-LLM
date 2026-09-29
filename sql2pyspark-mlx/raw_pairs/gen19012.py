from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select("last_restock")
    .union(df_shipments.select("delivered_date"))
    .orderBy("last_restock")
)
