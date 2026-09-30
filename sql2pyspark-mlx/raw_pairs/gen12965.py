from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.city").alias("city"), F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.quantity")).alias("max_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("region").desc())
)
