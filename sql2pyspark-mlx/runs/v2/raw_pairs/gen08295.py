from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity").between(95, 174))
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 62)
    .orderBy(F.col("stddev_quantity").desc())
)
