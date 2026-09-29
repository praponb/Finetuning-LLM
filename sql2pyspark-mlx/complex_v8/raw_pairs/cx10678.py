from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_uni = Window.partitionBy("warehouse_id", "operator").orderBy("unit_price_bucket")
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at_date")
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at_date", "id")
w_operator_by_unit_price_bucke = Window.partitionBy("operator").orderBy(F.col("unit_price_bucket").desc(), "id")
w_moved_at_date_seq_move_type = Window.partitionBy("moved_at_date_seq", "move_type")
w_all_by_first_qty_prior_6 = Window.orderBy("first_qty", "id").rowsBetween(-6, -1)
w_distinct_sku_id_in_move_type = Window.partitionBy("distinct_sku_id_in_move_type", "moved_at_date_seq").orderBy("prev_qty", "id")

base_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 125.9) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

sequenced = (
    base_moves
    .select(
        "id",
        "move_type",
        "qty",
        "operator",
        F.dense_rank().over(w_warehouse_id_operator_by_uni).alias("unit_price_bucket_dense_rank"),
        F.size(F.collect_set("warehouse_id").over(w_operator_move_type)).alias("distinct_warehouse_id_in_operator_move_type"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator_move_type), 2).alias("qty_vs_operator_move_type_avg"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.round(F.cume_dist().over(w_warehouse_id_move_type_by_mo), 3).alias("moved_at_date_cume_dist"),
        F.min("qty").over(w_operator_move_type).alias("operator_move_type_min_qty"),
        F.first("qty").over(w_all_by_qty_full).alias("first_qty"),
        F.row_number().over(w_warehouse_id_move_type_by_mo_2).alias("moved_at_date_seq"),
        F.lag("qty", 1, 0).over(w_operator_by_unit_price_bucke).alias("prev_qty"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "move_type",
        "qty",
        "operator",
        "distinct_warehouse_id_in_operator_move_type",
        "qty_vs_operator_move_type_avg",
        "distinct_sku_id_in_move_type",
        "moved_at_date_cume_dist",
        "first_qty",
        "moved_at_date_seq",
        F.round(F.avg("first_qty").over(w_moved_at_date_seq_move_type), 2).alias("moved_at_date_seq_move_type_avg_first_qty"),
        F.round(F.avg("qty_vs_move_type_avg").over(w_all_by_first_qty_prior_6), 2).alias("qty_vs_move_type_avg_avg_6_prior"),
        F.round(F.avg("operator_move_type_min_qty").over(w_moved_at_date_seq_move_type), 2).alias("moved_at_date_seq_move_type_avg_operator_move_ty"),
        F.ntile(3).over(w_distinct_sku_id_in_move_type).alias("prev_qty_tercile"),
    )
)

result = (
    with_trends
    .filter(~F.col("move_type").isin("INBOUND", "TRANSFER"))
    .select(
        "id",
        "qty",
        "operator",
        "qty_vs_operator_move_type_avg",
        "distinct_sku_id_in_move_type",
        "moved_at_date_cume_dist",
        "moved_at_date_seq",
        "moved_at_date_seq_move_type_avg_first_qty",
        "qty_vs_move_type_avg_avg_6_prior",
        "moved_at_date_seq_move_type_avg_operator_move_ty",
        "prev_qty_tercile",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("moved_at_date_seq").cast("string")).alias("move_type_ref"),
    )
    .orderBy(F.col("move_type_prefix").asc_nulls_last())
)
