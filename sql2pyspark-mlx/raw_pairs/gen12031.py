from pyspark.sql import functions as F

dim_totals = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .cube(F.col("p.status").alias("status"), F.col("p.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
        F.min(F.col("s.unit_price")).alias("min_unit_price"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "total_quantity",
        F.when(F.col("channel").rlike("^(partner|store)$"), "GROUP_A").when(F.col("channel").rlike("^(web|mobile)$"), "OTHER_ZONE").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
