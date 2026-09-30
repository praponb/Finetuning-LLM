from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_move_type_len = Window.partitionBy(F.date_trunc("month", "moved_at"), "move_type_len")
w_sku_id = Window.partitionBy("sku_id")
w_moved_at_date_year_by_unit_p = Window.partitionBy(F.year("moved_at_date")).orderBy("unit_price")
w_moved_at_date_month_warehous = Window.partitionBy(F.date_trunc("month", "moved_at_date"), "warehouse_id").orderBy(F.col("qty").desc_nulls_first(), "id")
w_moved_at_date_year_by_unit_p_2 = Window.partitionBy(F.year("moved_at_date")).orderBy("unit_price", "id")
w_warehouse_id_by_move_type_le = Window.partitionBy("warehouse_id").orderBy(F.col("move_type_len").asc_nulls_last(), "id")
w_warehouse_id_by_move_type_le_2 = Window.partitionBy("warehouse_id").orderBy(F.col("move_type_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at_date = Window.partitionBy("sku_id").orderBy(F.col("moved_at_date").desc(), "id")
w_distinct_move_type_in_moved = Window.partitionBy("distinct_move_type_in_moved_at_month_move_type_l").orderBy(F.col("unit_price").desc(), "id")
w_rn_by_unit_price_running = Window.partitionBy("rn").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_date_month_move_typ = Window.partitionBy(F.month("moved_at_date"), "move_type_len_dense_rank")

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

combined_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.upper(F.trim(F.col("sk.sku_code"))).like("E%"))
    .select("s.*", "sk.sku_code")
)

sequenced = (
    combined_details_sku
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_moved_at_month_move_type_len)).alias("distinct_sku_id_in_moved_at_month_move_type_len"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_month_move_type_len), F.lit(0)), 4).alias("qty_share_of_moved_at_month_move_type_len"),
        F.rank().over(w_moved_at_date_year_by_unit_p).alias("unit_price_rank"),
        F.lag("moved_at").over(w_moved_at_date_month_warehous).alias("prev_moved_at"),
        F.round(F.cume_dist().over(w_moved_at_date_month_warehous), 3).alias("qty_cume_dist"),
        F.size(F.collect_set("move_type").over(w_moved_at_month_move_type_len)).alias("distinct_move_type_in_moved_at_month_move_type_l"),
        F.lag("moved_at").over(w_moved_at_date_year_by_unit_p_2).alias("prev_moved_at_2"),
        F.dense_rank().over(w_warehouse_id_by_move_type_le).alias("move_type_len_dense_rank"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.max("unit_price").over(w_warehouse_id_by_move_type_le_2).alias("max_unit_price_so_far"),
        F.row_number().over(w_sku_id_by_moved_at_date).alias("rn"),
    )
)

leaders = sequenced.filter(F.col("rn") <= 5)

with_windows = (
    leaders
    .select(
        "*",
        (F.col("qty") - F.lag("qty").over(w_distinct_move_type_in_moved)).alias("qty_change"),
        F.sum("qty_share_of_sku_id").over(w_rn_by_unit_price_running).alias("running_qty_share_of_sku_id"),
        F.size(F.collect_set("move_type").over(w_moved_at_date_month_move_typ)).alias("distinct_move_type_in_moved_at_date_month_move_t"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_len",
        "sku_code",
        "distinct_sku_id_in_moved_at_month_move_type_len",
        "qty_share_of_sku_id",
        "qty_share_of_moved_at_month_move_type_len",
        "prev_moved_at",
        "distinct_move_type_in_moved_at_month_move_type_l",
        "prev_moved_at_2",
        "move_type_len_dense_rank",
        "sku_id_min_unit_price",
        "max_unit_price_so_far",
        "qty_change",
        "running_qty_share_of_sku_id",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy("prev_moved_at", "running_qty_share_of_sku_id")
)
