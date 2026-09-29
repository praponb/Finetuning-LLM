from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("city").isin(["Berlin", "London", "Bangkok"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.quantity").between(114, 239))
    .select(F.col("i.id"), F.col("i.store_id"))
    .orderBy(F.col("id").desc())
)
