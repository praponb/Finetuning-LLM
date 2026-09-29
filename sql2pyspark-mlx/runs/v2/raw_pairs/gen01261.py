from pyspark.sql import functions as F

avg_quantity = df_inventory.agg(F.avg("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > avg_quantity)
    .select("id", "quantity", "store_id", "last_restock")
    .orderBy(F.col("store_id").desc())
)
