from pyspark.sql import functions as F

src = df_inventory.filter((F.col("store_id") == 5) & (F.col("store_id") == 6))

agg = (
    src
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    agg
    .select("last_restock_year", "avg_quantity")
    .orderBy("avg_quantity")
)
