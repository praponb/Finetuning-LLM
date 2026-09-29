from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") <= "2024-11-05") & F.col("quantity").between(79, 152))
)

store_id_summary = (
    base
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    store_id_summary
    .filter(F.col("stddev_quantity") > 80)
    .select("store_id", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc(), "store_id")
)
