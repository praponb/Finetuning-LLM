from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.region").isin(["South", "East", "Central"]) & F.col("s.city").isin(["Chiang Mai", "Berlin", "Bangkok"]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("s.opened_date"))
)
