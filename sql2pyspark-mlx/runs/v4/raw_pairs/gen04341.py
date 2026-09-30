from pyspark.sql import functions as F

base = df_inventory.filter((F.col("quantity") == 113) & (F.col("quantity") >= 156))

agg = (
    base
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    agg
    .filter(F.col("quantity_range") > 36)
    .select("store_id", "quantity_range")
    .orderBy("quantity_range")
)
