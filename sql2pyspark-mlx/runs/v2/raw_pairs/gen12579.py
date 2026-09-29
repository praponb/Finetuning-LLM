from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity") <= 205).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.length(F.col("s.store_name")) >= 14)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.city"))
    .orderBy("city", "id")
    .limit(3)
)
