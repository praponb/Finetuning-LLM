from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.quantity") <= 112)
    .select(F.col("s.sqft"), F.col("i.quantity"))
    .orderBy("quantity")
)
