from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "channel"], ["region"]],
        F.col("p.region").alias("region"),
        F.col("src.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.amount")).alias("min_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("min_amount").desc_nulls_first())
)
