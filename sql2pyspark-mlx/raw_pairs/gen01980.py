from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)
