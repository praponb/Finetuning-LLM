from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price")
w_warehouse_id_sku_id_by_unit_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_unit_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price", "id")
w_sku_id_move_count_by_max_mov = Window.partitionBy("sku_id", "move_count").orderBy(F.col("max_move_qty").desc())
w_sku_id_move_count_by_max_mov_2 = Window.partitionBy("sku_id", "move_count").orderBy(F.col("max_move_qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_parent = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.col("sk.unit_cost") >= 134.3)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "sku_unit_cost",
        "warehouse_wh_code",
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_unit), 3).alias("unit_price_cume_dist"),
        F.first("unit_price").over(w_warehouse_id_sku_id_by_unit_2).alias("first_unit_price"),
        F.lead("qty").over(w_warehouse_id_sku_id_by_unit_3).alias("next_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("qty").alias("max_move_qty"),
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.max_move_qty",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

ranked = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "warehouse_wh_code",
        "unit_price_cume_dist",
        "next_qty",
        "max_move_qty",
        "move_operator_count",
        "move_count",
        F.round(F.cume_dist().over(w_sku_id_move_count_by_max_mov), 3).alias("max_move_qty_cume_dist"),
        F.lead("first_unit_price").over(w_sku_id_move_count_by_max_mov_2).alias("next_first_unit_price"),
        F.dense_rank().over(w_sku_id_move_count_by_max_mov).alias("max_move_qty_dense_rank"),
        F.min("move_operator_count").over(w_warehouse_id).alias("warehouse_id_min_move_operator_count"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.concat(F.col("warehouse_wh_code"), F.lit("#"), F.col("move_count").cast("string")).alias("warehouse_wh_code_ref"),
    )
)

result = (
    derived
    .select(
        "id",
        "warehouse_id",
        "warehouse_wh_code",
        "unit_price_cume_dist",
        "next_qty",
        "max_move_qty",
        "move_operator_count",
        "move_count",
        "max_move_qty_cume_dist",
        "max_move_qty_dense_rank",
        "warehouse_wh_code_ref",
        F.when(F.col("max_move_qty_cume_dist") >= 0.8, "A").when(F.col("max_move_qty_cume_dist") >= 0.2, "B").otherwise("C").alias("max_move_qty_cume_dist_tier"),
        F.concat(F.col("warehouse_wh_code_ref"), F.lit("#"), F.col("next_qty").cast("string")).alias("warehouse_wh_code_ref_ref"),
    )
    .orderBy(F.col("id").desc())
)
