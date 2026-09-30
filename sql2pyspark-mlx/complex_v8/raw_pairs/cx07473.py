from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("move_type_len", "id")
w_all_by_sku_unit_cost_running = Window.orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_move_type_by_move_type_len = Window.partitionBy("move_type").orderBy("move_type_len", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("move_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_move_type_by_mo_3 = Window.partitionBy("warehouse_id", "move_type").orderBy("move_type_len")

scoped_moves = (
    df_stock_moves
    .filter((F.col("qty") < 357) & (F.col("move_type") != "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

with_parent = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_len",
        "s.qty_plus_unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "move_type_len",
        "qty_plus_unit_price",
        "sku_unit_cost",
        "sku_labels",
        "warehouse_wh_code",
        F.lag("sku_unit_cost").over(w_warehouse_id_move_type_by_mo).alias("prev_sku_unit_cost"),
        F.first("unit_price", ignorenulls=True).over(w_all_by_sku_unit_cost_running).alias("first_known_unit_price"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_sku_id_move_type), F.lit(0)), 4).alias("sku_unit_cost_share_of_sku_id_move_type"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_move_type), F.lit(0)), 4).alias("unit_price_share_of_sku_id_move_type"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.ntile(4).over(w_warehouse_id_move_type_by_mo).alias("move_type_len_quartile"),
        F.lag("unit_price").over(w_move_type_by_move_type_len).alias("prev_unit_price"),
        F.round(F.col("qty_plus_unit_price") / F.nullif(F.sum("qty_plus_unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("qty_plus_unit_price_share_of_warehouse_id"),
        F.first("qty", ignorenulls=True).over(w_warehouse_id_move_type_by_mo_2).alias("first_known_qty"),
        F.dense_rank().over(w_warehouse_id_move_type_by_mo_3).alias("move_type_len_dense_rank"),
        F.ntile(10).over(w_move_type_by_move_type_len).alias("move_type_len_decile"),
    )
)

result = (
    windowed
    .filter(F.col("prev_unit_price") <= 367.7)
    .select(
        "id",
        "qty",
        "move_type_len",
        "qty_plus_unit_price",
        "sku_labels",
        "prev_sku_unit_cost",
        "first_known_unit_price",
        "sku_unit_cost_share_of_sku_id_move_type",
        "unit_price_share_of_sku_id_move_type",
        "rows_in_sku_id_move_type",
        "prev_unit_price",
        "qty_plus_unit_price_share_of_warehouse_id",
        "move_type_len_dense_rank",
        "move_type_len_decile",
        F.substring(F.trim(F.col("warehouse_wh_code")), 1, 2).alias("warehouse_wh_code_prefix"),
        F.round(F.col("sku_unit_cost") * 1.5, 2).alias("sku_unit_cost_adj"),
    )
    .orderBy(F.col("rows_in_sku_id_move_type").desc(), "id")
    .limit(5)
)
