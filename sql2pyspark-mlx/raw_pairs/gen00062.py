from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.quantity")).alias("total_quantity"),
        F.grouping_id().alias("level_id"),
    )
)
