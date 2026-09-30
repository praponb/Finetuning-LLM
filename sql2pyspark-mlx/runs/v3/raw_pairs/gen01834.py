from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy("last_restock_month")
    .limit(5)
)
