from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("region").isin(["West", "North", "East"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
)
