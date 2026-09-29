from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 56)
)
