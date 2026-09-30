from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .cube(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.quantity")).alias("min_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
