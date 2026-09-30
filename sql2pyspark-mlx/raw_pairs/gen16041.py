from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .intersect(df_inventory.select("last_restock"))
)
