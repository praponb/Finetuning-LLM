from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_move_moved_at_ties = Window.partitionBy("sku_id").orderBy(F.col("move_moved_at").asc_nulls_last())
w_sku_id_by_move_moved_at = Window.partitionBy("sku_id").orderBy(F.col("move_moved_at").asc_nulls_last(), "id", "move_id")

prepared = (
    df_stock_moves
    .filter(F.col("operator") != "op_dew")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

derived = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

line_level_moves = (
    derived.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.operator_clean",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

with_trends = (
    line_level_moves
    .select(
        "*",
        F.rank().over(w_sku_id_by_move_moved_at_ties).alias("move_moved_at_rank"),
        F.round(F.cume_dist().over(w_sku_id_by_move_moved_at_ties), 3).alias("move_moved_at_cume_dist"),
        F.ntile(3).over(w_sku_id_by_move_moved_at).alias("move_moved_at_tercile"),
    )
)

detail_rows_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.move_sku_id")) & (F.col("st.warehouse_id") <= 2), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.operator_clean",
        "s.move_id",
        "s.move_operator",
        "s.move_sku_id",
        "s.move_moved_at",
        "s.move_moved_at_rank",
        "s.move_moved_at_cume_dist",
        F.col("st.id").alias("move_id_2"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

enriched_move = (
    detail_rows_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id_2"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.operator_clean",
        "s.move_id",
        "s.move_operator",
        "s.move_sku_id",
        "s.move_moved_at",
        "s.move_moved_at_rank",
        "s.move_moved_at_cume_dist",
        "s.move_id_2",
        "s.move_moved_at_2",
        "s.move_warehouse_id",
        F.col("st.warehouse_id").alias("move_warehouse_id_2"),
    )
)

with_details = (
    enriched_move.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.move_warehouse_id_2"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.operator_clean",
        "s.move_id",
        "s.move_operator",
        "s.move_sku_id",
        "s.move_moved_at",
        "s.move_moved_at_rank",
        "s.move_moved_at_cume_dist",
        "s.move_id_2",
        "s.move_moved_at_2",
        "s.move_warehouse_id",
        "s.move_warehouse_id_2",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "operator",
        "move_id",
        "move_sku_id",
        "move_moved_at",
        "move_moved_at_rank",
        "move_moved_at_cume_dist",
        "move_id_2",
        "move_moved_at_2",
        "move_warehouse_id",
        "move_warehouse_id_2",
        "warehouse_capacity",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
    .orderBy(F.col("move_moved_at").desc(), "move_moved_at_cume_dist")
)
