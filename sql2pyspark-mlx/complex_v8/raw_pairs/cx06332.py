from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc())

base_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 19) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

without_matches = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("K%")) & (F.col("w.capacity").between(2614, 11506)), "left_semi")
)

labeled = (
    without_matches
    .select(
        "*",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean_2"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "operator_clean",
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.row_number().over(w_sku_id_by_moved_at).alias("moved_at_seq"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.round(F.cume_dist().over(w_sku_id_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("unit_price").alias("max_move_unit_price"))
)

with_counts = (
    with_windows.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.max_move_unit_price")
)

result = (
    with_counts
    .filter(F.col("distinct_sku_id_in_warehouse_id") <= 6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator_clean",
        "rows_in_warehouse_id",
        "moved_at_seq",
        "distinct_sku_id_in_warehouse_id",
        "moved_at_cume_dist",
    )
    .orderBy(F.col("moved_at_cume_dist").asc())
)
