from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_move_type_by_mo_3 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at_quarter", "id")
w_moved_at_quarter_moved_at_qu = Window.partitionBy("moved_at_quarter", "moved_at_quarter_seq")
w_operator_moved_at_quarter_se = Window.partitionBy("operator", "moved_at_quarter_seq").orderBy("second_qty", "id")
w_operator_moved_at_quarter_se_2 = Window.partitionBy("operator", "moved_at_quarter_seq").orderBy("second_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_slug",
        "moved_at_quarter",
        "moved_at_label",
        F.nth_value("qty", 2).over(w_warehouse_id_move_type_by_mo).alias("second_qty"),
        F.first("qty", ignorenulls=True).over(w_warehouse_id_move_type_by_mo_2).alias("first_known_qty"),
        F.round(F.col("moved_at_quarter") - F.avg("moved_at_quarter").over(w_sku_id), 2).alias("moved_at_quarter_vs_sku_id_avg"),
        F.row_number().over(w_warehouse_id_move_type_by_mo_3).alias("moved_at_quarter_seq"),
        F.sum("unit_price").over(w_warehouse_id_move_type_by_mo_2).alias("running_unit_price"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.round(F.col("moved_at_quarter_seq") - F.avg("moved_at_quarter_seq").over(w_moved_at_quarter_moved_at_qu), 2).alias("moved_at_quarter_seq_vs_moved_at_quarter_moved_a"),
        F.round(F.avg("running_unit_price").over(w_moved_at_quarter_moved_at_qu), 2).alias("moved_at_quarter_moved_at_quarter_seq_avg_runnin"),
        F.min("qty").over(w_moved_at_quarter_moved_at_qu).alias("moved_at_quarter_moved_at_quarter_seq_min_qty"),
        F.ntile(3).over(w_operator_moved_at_quarter_se).alias("second_qty_tercile"),
        F.last("moved_at_quarter_seq").over(w_operator_moved_at_quarter_se_2).alias("last_moved_at_quarter_seq"),
    )
)

result = (
    with_windows_2
    .groupBy("moved_at", "second_qty_tercile")
    .pivot("operator", ["op_chan", "op_eve"])
    .agg(F.max("moved_at_quarter").alias("max_v"), F.count("moved_at_quarter").alias("n"))
)
