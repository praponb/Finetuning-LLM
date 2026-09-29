from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("city").isin(["Chiang Mai", "Bangkok", "Singapore"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.quantity") > 125)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
    )
)
