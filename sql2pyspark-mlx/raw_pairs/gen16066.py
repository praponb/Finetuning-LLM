from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.channel").alias("channel"), F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.unit_price")), 2).alias("total_unit_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
