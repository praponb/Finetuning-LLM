from pyspark.sql import functions as F

src = df_inventory.filter(F.col("last_restock") >= "2024-03-01")

agg = (
    src
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = agg.orderBy(F.col("store_id").desc(), "avg_quantity")
