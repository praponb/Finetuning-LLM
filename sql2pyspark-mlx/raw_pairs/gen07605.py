from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["channel", "status"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.unit_price")).alias("min_unit_price"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("min_unit_price").desc_nulls_last())
)
