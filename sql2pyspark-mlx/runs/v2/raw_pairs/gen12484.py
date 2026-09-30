from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.product_id") == 1)
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.region"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("avg_quantity", F.col("max_quantity").desc())
)
