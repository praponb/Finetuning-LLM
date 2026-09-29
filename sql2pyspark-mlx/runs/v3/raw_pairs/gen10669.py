from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 52)
    .orderBy("avg_quantity", "last_restock_year")
)
