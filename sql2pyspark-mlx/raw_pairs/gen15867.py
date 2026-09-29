from pyspark.sql import functions as F

rollup_totals = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .rollup(F.col("o.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.unit_price")), 2).alias("total_unit_price"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    rollup_totals
    .select(
        "status",
        "total_unit_price",
        "level_id",
        F.when(F.col("status").rlike("^(PENDING|RETURNED)$"), "PRIORITY").when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("status_zone"),
    )
)
