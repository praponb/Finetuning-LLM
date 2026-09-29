from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.product_id").isin([1, 5]) & (F.col("s.sqft") > 10374))
    .select(F.col("s.region"), F.col("s.opened_date"), F.col("i.product_id"))
)
