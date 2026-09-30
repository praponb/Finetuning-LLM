from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([3, 5, 7])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.store_name").like("%Old%"))
    .select(F.col("s.id"), F.col("s.opened_date"))
)
