from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"), "store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)
