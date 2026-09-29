from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.upper("store_name").like("%IRP%")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("i.id"), F.col("i.store_id"))
    .orderBy(F.col("id").desc())
)
