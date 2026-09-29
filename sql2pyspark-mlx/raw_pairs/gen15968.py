from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([1, 2, 3, 6])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.opened_date") < "2024-12-08")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.city"))
)
