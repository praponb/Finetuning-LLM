from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("qty_filled").desc(), F.col("id").desc(), F.col("move_id").desc())
w_latest_2 = Window.partitionBy("move_operator", "sku_id").orderBy(F.col("qty").desc(), F.col("id").desc(), F.col("move_id").desc(), F.col("move_id_2").desc())
w_move_id = Window.partitionBy("move_id")
w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy("moved_at", "id", "move_id", "move_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

line_level = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.moved_at_label",
        "s.qty_filled",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

latest = (
    line_level
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

detail_rows = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.moved_at_label",
        "s.qty_filled",
        "s.move_id",
        "s.move_type",
        "s.move_unit_price",
        F.col("st.id").alias("move_id_2"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

deduped = (
    detail_rows
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    deduped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "moved_at_label",
        "qty_filled",
        "move_id",
        "move_type",
        "move_unit_price",
        "move_id_2",
        "move_operator",
        "move_moved_at_2",
        "move_move_type",
        "move_warehouse_id",
        F.round(F.col("move_unit_price") - F.avg("move_unit_price").over(w_move_id), 2).alias("move_unit_price_vs_move_id_avg"),
        F.size(F.collect_set("move_move_type").over(w_move_id)).alias("distinct_move_move_type_in_move_id"),
        F.size(F.collect_set("move_operator").over(w_move_id)).alias("distinct_move_operator_in_move_id"),
        F.size(F.collect_set("sku_id").over(w_move_id)).alias("distinct_sku_id_in_move_id"),
        F.nth_value("move_unit_price", 2).over(w_warehouse_id_by_moved_at_ful).alias("second_move_unit_price"),
        F.sum("move_unit_price").over(w_move_id).alias("move_id_sum_move_unit_price"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
        F.initcap(F.trim(F.col("move_operator"))).alias("move_operator_clean"),
    )
)

result = (
    labeled
    .groupBy("move_moved_at_2")
    .pivot("move_move_type", ["ADJUST", "INBOUND"])
    .agg(F.max("move_id_sum_move_unit_price"))
    .orderBy("move_moved_at_2")
)
