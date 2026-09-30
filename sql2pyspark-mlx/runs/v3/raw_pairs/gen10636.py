from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft").between(9146, 12845)).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.quantity") <= 154)
    .select(F.col("i.id"), F.col("i.product_id"))
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
)
