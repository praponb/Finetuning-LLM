from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([8, 11, 12]))
    .select("delivered_date")
    .intersect(df_inventory.select("last_restock"))
    .orderBy("delivered_date")
)
