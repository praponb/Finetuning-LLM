from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id_by_move_moved_at = Window.partitionBy("move_id").orderBy("move_moved_at", "id", "move_id")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_by_move_qty = Window.partitionBy("warehouse_id").orderBy("move_qty", "id", "move_id")

recent_moves = (
    df_stock_moves
    .filter((F.col("qty") > 189) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("unit_price") >= 307.8, "large").when(F.col("unit_price") >= 160.8, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

with_children_moves = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.operator_slug",
        "s.unit_price_tier",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

ranked = (
    with_children_moves
    .select(
        "*",
        F.lead("move_moved_at", 2).over(w_move_id_by_move_moved_at).alias("next2_move_moved_at"),
        F.round(F.avg("move_qty").over(w_sku_id), 2).alias("sku_id_avg_move_qty"),
        F.round(F.cume_dist().over(w_warehouse_id_by_move_qty), 3).alias("move_qty_cume_dist"),
    )
)

detail_rows_moves = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.operator_slug",
        "s.unit_price_tier",
        "s.move_id",
        "s.move_operator",
        "s.move_qty",
        "s.move_moved_at",
        "s.next2_move_moved_at",
        F.col("st.id").alias("move_id_2"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        "st.move_type",
        F.col("st.qty").alias("move_qty_2"),
    )
)

annotated = (
    detail_rows_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator_slug",
        "unit_price_tier",
        "move_id",
        "move_operator",
        "move_qty",
        "move_moved_at",
        "move_id_2",
        "move_moved_at_2",
        F.least(F.col("move_qty_2"), F.col("move_qty")).alias("least_move_qty_2_move_qty"),
    )
)

result = (
    annotated
    .select(
        "id",
        "move_id",
        "least_move_qty_2_move_qty",
        "move_qty",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["least_move_qty_2_move_qty", "move_qty"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "move_id", "metric")
)
