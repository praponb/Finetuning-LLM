from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select("last_restock")
    .union(df_shipments.select("shipped_date"))
    .distinct()
    .orderBy("last_restock")
)
