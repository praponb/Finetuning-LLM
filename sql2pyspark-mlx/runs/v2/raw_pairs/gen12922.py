from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .filter(F.col("ibs.unique_stores") > 5)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ibs.unique_stores"),
    )
    .orderBy(F.col("sqft").desc())
)
