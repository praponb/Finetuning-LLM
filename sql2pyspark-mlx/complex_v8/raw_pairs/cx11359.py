from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_by_qty_running = Window.partitionBy("move_operator").orderBy("qty", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_move_unit_price = Window.orderBy("move_unit_price", "id", "move_id")
w_sku_id_by_unit_price_filled = Window.partitionBy("sku_id").orderBy("unit_price_filled", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_unit_price_quartile_sku = Window.partitionBy("move_unit_price_quartile", "sku_id")
w_move_move_type_sku_id = Window.partitionBy("move_move_type", "sku_id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id", "move_id")
w_move_unit_price_quartile_mov = Window.partitionBy("move_unit_price_quartile", "move_move_type").orderBy("move_unit_price_quartile", "id", "move_id")
w_move_unit_price_quartile_mov_2 = Window.partitionBy("move_unit_price_quartile", "move_move_type").orderBy("move_unit_price_quartile", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_unit_price_quartile_mov_3 = Window.partitionBy("move_unit_price_quartile", "move_move_type").orderBy("move_unit_price_quartile")

base_moves = (
    df_stock_moves
    .filter(F.col("qty").between(91, 463))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

exploded_detail_moves = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price_filled",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

sequenced = (
    exploded_detail_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price_filled",
        "move_id",
        "move_operator",
        "move_move_type",
        F.sum("move_unit_price").over(w_move_operator_by_qty_running).alias("running_move_unit_price"),
        F.ntile(4).over(w_all_by_move_unit_price).alias("move_unit_price_quartile"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.last("qty", ignorenulls=True).over(w_sku_id_by_unit_price_filled).alias("last_known_qty"),
        F.min("qty").over(w_move_unit_price_quartile_sku).alias("move_unit_price_quartile_sku_id_min_qty"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "move_id",
        "move_operator",
        "move_move_type",
        "running_move_unit_price",
        "move_unit_price_quartile",
        "move_unit_price_quartile_sku_id_min_qty",
        F.count(F.lit(1)).over(w_move_move_type_sku_id).alias("rows_in_move_move_type_sku_id"),
        F.round(F.cume_dist().over(w_warehouse_id_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.max("running_move_unit_price").over(w_move_move_type_sku_id).alias("move_move_type_sku_id_max_running_move_unit_pric"),
    )
)

with_trends = (
    with_windows_2
    .select(
        "id",
        "sku_id",
        "moved_at",
        "move_id",
        "move_move_type",
        "running_move_unit_price",
        "move_unit_price_quartile",
        "move_unit_price_quartile_sku_id_min_qty",
        "rows_in_move_move_type_sku_id",
        "moved_at_cume_dist",
        "move_move_type_sku_id_max_running_move_unit_pric",
        F.rank().over(w_move_unit_price_quartile_mov).alias("move_unit_price_quartile_rank"),
        F.last("move_unit_price_quartile").over(w_move_unit_price_quartile_mov_2).alias("last_move_unit_price_quartile"),
        F.rank().over(w_move_unit_price_quartile_mov_3).alias("move_unit_price_quartile_rank_2"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.move_id",
        "s.move_move_type",
        "s.running_move_unit_price",
        "s.move_unit_price_quartile",
        "s.move_unit_price_quartile_sku_id_min_qty",
        "s.rows_in_move_move_type_sku_id",
        "s.move_unit_price_quartile_rank",
        "s.last_move_unit_price_quartile",
        "s.move_unit_price_quartile_rank_2",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "sku_id",
        "moved_at",
        "move_move_type",
        "running_move_unit_price",
        "move_unit_price_quartile",
        "move_unit_price_quartile_sku_id_min_qty",
        "last_move_unit_price_quartile",
        "sku_weight_kg",
        "sku_unit_cost",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
    )
    .orderBy(F.col("sku_id").desc_nulls_first())
)
