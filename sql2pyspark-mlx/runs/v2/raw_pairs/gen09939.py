from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity").isNotNull()).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.city").isin(["Chiang Mai", "Bangkok"]))
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.opened_date"))
    .orderBy(F.col("sqft").desc())
)
