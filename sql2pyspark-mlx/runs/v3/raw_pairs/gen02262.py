from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_inventory.filter(F.col("product_id") == 2).select("last_restock"))
    .orderBy(F.col("delivered_date").desc())
)
