from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price_adj").desc(), "id", "move_id")
w_warehouse_id_move_id_by_move = Window.partitionBy("warehouse_id", "move_id").orderBy("move_qty", "id", "move_id")
w_operator_move_warehouse_id = Window.partitionBy("operator", "move_warehouse_id")
w_move_id_2_by_qty_moving_1 = Window.partitionBy("move_id_2").orderBy("qty", "id", "move_id", "move_id_2").rowsBetween(-1, Window.currentRow)
w_move_id_2_by_qty_moving_2 = Window.partitionBy("move_id_2").orderBy("qty", "id", "move_id", "move_id_2").rowsBetween(-2, Window.currentRow)

scoped = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

line_level_moves = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price_adj",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

metrics = (
    line_level_moves
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price_adj",
        "move_id",
        "move_qty",
        "move_warehouse_id",
        "move_unit_price",
        (F.col("move_unit_price") - F.lag("move_unit_price").over(w_warehouse_id_sku_id_by_unit)).alias("move_unit_price_change"),
        F.lead("unit_price_adj").over(w_warehouse_id_move_id_by_move).alias("next_unit_price_adj"),
    )
)

tagged = (
    metrics
    .filter(F.col("move_unit_price_change") >= -350.0)
    .select(
        "*",
        F.when(F.col("next_unit_price_adj") >= 562.8, "large").when(F.col("next_unit_price_adj") >= 263.1, "medium").otherwise("small").alias("next_unit_price_adj_tier"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_children_moves = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_unit_price_change",
        "s.next_unit_price_adj",
        "s.next_unit_price_adj_tier",
        F.col("st.id").alias("move_id_2"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        F.col("st.operator").alias("move_operator"),
        "st.move_type",
    )
)

ranked = (
    with_children_moves
    .select(
        "*",
        F.max("next_unit_price_adj").over(w_operator_move_warehouse_id).alias("operator_move_warehouse_id_max_next_unit_price_a"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_move_warehouse_id), F.lit(0)), 4).alias("qty_share_of_operator_move_warehouse_id"),
        F.round(F.col("move_unit_price_2") / F.nullif(F.sum("move_unit_price_2").over(w_operator_move_warehouse_id), F.lit(0)), 4).alias("move_unit_price_2_share_of_operator_move_warehou"),
        F.round(F.avg("next_unit_price_adj").over(w_move_id_2_by_qty_moving_1), 2).alias("next_unit_price_adj_avg_2"),
        F.round(F.avg("qty").over(w_move_id_2_by_qty_moving_2), 2).alias("qty_avg_3"),
        F.size(F.collect_set("move_id").over(w_operator_move_warehouse_id)).alias("distinct_move_id_in_operator_move_warehouse_id"),
    )
)

result = (
    ranked
    .groupBy("move_id", "next_unit_price_adj_tier")
    .pivot("move_operator", ["op_bee", "op_dew", "op_eve", "op_ann"])
    .agg(F.sum("move_unit_price_2_share_of_operator_move_warehou"))
)
