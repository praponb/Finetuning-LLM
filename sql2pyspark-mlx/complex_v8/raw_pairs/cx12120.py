from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_move = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("move_moved_at").desc())
w_warehouse_id_move_operator = Window.partitionBy("warehouse_id", "move_operator")
w_sku_id_warehouse_id_by_move_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("move_moved_at").desc(), "id", "move_id")

valid_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label_2"),
    )
)

detail_rows_moves = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.moved_at_label",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

windowed = (
    detail_rows_moves
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at_label",
        "move_id",
        "move_moved_at",
        F.dense_rank().over(w_sku_id_warehouse_id_by_move).alias("move_moved_at_dense_rank"),
        F.round(F.col("move_qty") - F.avg("move_qty").over(w_warehouse_id_move_operator), 2).alias("move_qty_vs_warehouse_id_move_operator_avg"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id_move_operator), F.lit(0)), 4).alias("qty_share_of_warehouse_id_move_operator"),
        F.lag("move_moved_at").over(w_sku_id_warehouse_id_by_move_2).alias("prev_move_moved_at"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.qty",
        "s.moved_at_label",
        "s.move_id",
        "s.move_moved_at_dense_rank",
        "s.move_qty_vs_warehouse_id_move_operator_avg",
        "s.qty_share_of_warehouse_id_move_operator",
        "s.prev_move_moved_at",
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        "st.move_type",
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.month(F.col("st.moved_at")).isin(1, 2)), "left_anti")
)

screened = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 15), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "moved_at_label",
        "move_id",
        "move_moved_at_dense_rank",
        "qty_share_of_warehouse_id_move_operator",
        "prev_move_moved_at",
        "move_unit_price",
        "move_qty",
        "move_type",
        F.concat(F.col("moved_at_label"), F.lit("#"), F.col("move_moved_at_dense_rank").cast("string")).alias("moved_at_label_ref"),
        F.round(F.col("move_qty") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("move_qty_per_qty"),
    )
    .orderBy(F.col("move_unit_price").asc_nulls_last(), "id", "move_id")
    .limit(10)
)
