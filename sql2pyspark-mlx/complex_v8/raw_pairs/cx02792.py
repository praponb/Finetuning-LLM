from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_move_type_group_warehouse_id = Window.partitionBy("move_type_group", "warehouse_id").orderBy("unit_price", "id")
w_move_type_group_by_unit_pric = Window.partitionBy("move_type_group").orderBy("unit_price")

base_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

derived = (
    base_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.ntile(5).over(w_move_type_group_warehouse_id).alias("unit_price_quintile"),
        F.round(F.cume_dist().over(w_move_type_group_by_unit_pric), 3).alias("unit_price_cume_dist"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
    )
)

stats = (
    windowed
    .filter(F.col("rows_in_move_type") >= 10)
    .groupBy("move_type")
    .agg(
        F.sum(F.when(~F.col("move_type").isin("TRANSFER", "ADJUST"), 1).otherwise(0)).alias("n_move_type_not_in_transfer_ad"),
        F.round(100 * F.count(F.when(F.col("move_type") == "OUTBOUND", 1)) / F.count("*"), 1).alias("pct_move_type_outbound"),
        F.sum(F.when(F.col("move_type").isin("TRANSFER", "ADJUST", "OUTBOUND"), 1).otherwise(0)).alias("n_move_type_in_transfer_adjust"),
        F.max("unit_price_cume_dist").alias("max_unit_price_cume_dist"),
    )
)

scored = (
    stats
    .filter(F.col("n_move_type_in_transfer_adjust") < 8)
    .select(
        "*",
        F.when(F.col("max_unit_price_cume_dist") >= 0.7, "A").when(F.col("max_unit_price_cume_dist") >= 0.2, "B").otherwise("C").alias("max_unit_price_cume_dist_tier"),
    )
)

with_activity = (
    scored
    .crossJoin(scored.agg(F.avg("n_move_type_not_in_transfer_ad").alias("overall_avg_n_move_type_not_in_transfer_ad")))
    .filter(F.col("n_move_type_not_in_transfer_ad") > F.col("overall_avg_n_move_type_not_in_transfer_ad"))
    .drop("overall_avg_n_move_type_not_in_transfer_ad")
)

without_matches = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("n_move_type_in_transfer_adjust").alias("overall_avg_n_move_type_in_transfer_adjust")))
    .filter(F.col("n_move_type_in_transfer_adjust") > F.col("overall_avg_n_move_type_in_transfer_adjust"))
    .drop("overall_avg_n_move_type_in_transfer_adjust")
)

result = (
    without_matches
    .select(
        "move_type",
        "n_move_type_not_in_transfer_ad",
        "n_move_type_in_transfer_adjust",
        "max_unit_price_cume_dist",
        "max_unit_price_cume_dist_tier",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.round(F.col("pct_move_type_outbound") / F.nullif(F.col("max_unit_price_cume_dist"), F.lit(0)), 3).alias("pct_move_type_outbound_per_max_unit_price_cume_d"),
    )
    .orderBy(F.col("pct_move_type_outbound_per_max_unit_price_cume_d").asc())
)
