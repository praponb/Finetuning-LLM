from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity").between(71, 104)).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.opened_date"))
)
