from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price_bucket").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_dew", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix_2"),
    )
)

latest_per_key = (
    scoped
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest_per_key
    .filter(F.col("unit_price_bucket") > 76)
    .withColumn("moved_at_day", F.date_trunc("day", F.col("moved_at")))
)

labeled = (
    enriched
    .filter(F.col("unit_price") <= 329.7)
    .select(
        "*",
        F.when(F.col("operator").isin("op_bee", "op_dew"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
        F.coalesce(F.col("unit_price_bucket"), F.lit(0)).alias("unit_price_bucket_filled"),
        F.length(F.trim(F.col("move_type_prefix"))).alias("move_type_prefix_len"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("unit_price").alias("max_move_unit_price"))
)

with_stats = (
    labeled.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.max_move_unit_price")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("unit_price").alias("max_move_unit_price_2"),
    )
)

with_counts = (
    with_stats.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.avg_move_unit_price", "st.max_move_unit_price_2")
)

result = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "unit_price_bucket",
        "move_type_prefix",
        "unit_price_bucket_filled",
        "move_type_prefix_len",
        "max_move_unit_price",
        "avg_move_unit_price",
        "max_move_unit_price_2",
    )
)
