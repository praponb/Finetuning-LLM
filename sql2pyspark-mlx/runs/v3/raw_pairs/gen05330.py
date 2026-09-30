from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("store_name").like("%Town%")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.quantity") > 122)
    .select(F.col("i.id"), F.col("i.product_id"))
)
