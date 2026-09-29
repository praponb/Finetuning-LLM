from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_sku_id_by_qty_f = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_id_sku_id_by_qty = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty").desc(), "id")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy(F.col("qty").desc_nulls_first())
w_qty_seq_distinct_move_type_i = Window.partitionBy("qty_seq", "distinct_move_type_in_sku_id").orderBy("rows_in_sku_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_qty_seq_distinct_move_type_i_2 = Window.partitionBy("qty_seq", "distinct_move_type_in_sku_id").orderBy("rows_in_sku_id", "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 13:00:00").cast("timestamp")) & (F.col("move_type") == "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.move_type_ref",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "move_type_ref",
        "warehouse_wh_code",
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.first("qty").over(w_warehouse_id_sku_id_by_qty_f).alias("first_qty"),
        F.row_number().over(w_all_by_qty).alias("seq_no"),
        F.row_number().over(w_warehouse_id_sku_id_by_qty).alias("qty_seq"),
        F.min("qty").over(w_sku_id).alias("sku_id_min_qty"),
        F.dense_rank().over(w_sku_id_by_qty_ties).alias("qty_dense_rank"),
    )
)

top_rows = metrics.filter((F.col("qty_seq") == 1) & (F.col("first_qty") >= 139))

with_windows = (
    top_rows
    .select(
        "*",
        F.nth_value("first_qty", 2).over(w_qty_seq_distinct_move_type_i).alias("second_first_qty"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_warehouse_id), F.lit(0)), 4).alias("seq_no_share_of_warehouse_id"),
        F.max("qty_seq").over(w_qty_seq_distinct_move_type_i_2).alias("qty_seq_max_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "sku_id",
        "move_type_ref",
        "warehouse_wh_code",
        "seq_no",
        "qty_seq",
        "second_first_qty",
        "seq_no_share_of_warehouse_id",
        F.initcap(F.trim(F.col("move_type_ref"))).alias("move_type_ref_clean"),
    )
    .orderBy(F.col("second_first_qty").asc())
)
