from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_or_unknown_warehous = Window.partitionBy("operator_or_unknown", "warehouse_id").orderBy("moved_at", "id")

valid_moves = (
    df_stock_moves
    .filter(F.col("move_type").isin("INBOUND", "OUTBOUND", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.when(F.col("operator").isin("op_chan", "op_eve"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

ranked = (
    valid_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator_group",
        "operator_or_unknown",
        F.round(F.cume_dist().over(w_operator_or_unknown_warehous), 3).alias("moved_at_cume_dist"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_or_unknown_warehous))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

result = (
    ranked
    .groupingSets(
        [["move_type", "sku_id"], ["move_type"], []],
        "move_type", "sku_id",
    )
    .agg(
        F.percentile_approx("minutes_since_prev_moved_at", 0.5).alias("median_minutes_since_prev_moved_at"),
        F.percentile_approx("moved_at_cume_dist", 0.5).alias("median_moved_at_cume_dist"),
        F.round(100 * F.count(F.when(F.col("move_type").isin("ADJUST", "INBOUND"), 1)) / F.count("*"), 1).alias("pct_move_type_in_adjust_inbound"),
    )
)
