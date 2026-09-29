from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("inventory_count") > 4)
    .orderBy(F.col("stddev_quantity").desc())
)
