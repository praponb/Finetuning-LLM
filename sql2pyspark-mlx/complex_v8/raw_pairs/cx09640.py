from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_operator_sku_is_active_by_mo = Window.partitionBy("operator", "sku_is_active").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_move_type = Window.partitionBy("move_type")
w_all_by_total_unit_price_vs_s = Window.orderBy("total_unit_price_vs_sku_id_avg", "moved_at_seq")
w_min_moved_at_seq = Window.partitionBy("min_moved_at_seq")

filtered = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("qty").between(137, 426)))
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

combined_details = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.sum("qty").over(w_sku_id).alias("sku_id_sum_qty"),
        F.first("unit_price", ignorenulls=True).over(w_operator_sku_is_active_by_mo).alias("first_known_unit_price"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.row_number().over(w_all_by_moved_at).alias("moved_at_seq"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.max("qty").over(w_move_type).alias("move_type_max_qty"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
    )
)

leaders = sequenced.filter(F.col("moved_at_seq") == 1)

summary = (
    leaders
    .groupBy("moved_at_seq")
    .agg(
        F.min("moved_at_seq").alias("min_moved_at_seq"),
        F.sum("unit_price_vs_sku_id_avg").alias("total_unit_price_vs_sku_id_avg"),
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("n_operator_is_not_null"),
        F.max("unit_price").alias("max_unit_price_check"),
    )
    .filter(F.col("max_unit_price_check") > 189.4)
    .drop("max_unit_price_check")
)

with_windows = (
    summary
    .select(
        "moved_at_seq",
        "min_moved_at_seq",
        "total_unit_price_vs_sku_id_avg",
        "n_operator_is_not_null",
        F.lag("moved_at_seq").over(w_all_by_total_unit_price_vs_s).alias("prev_moved_at_seq"),
        F.sum("moved_at_seq").over(w_min_moved_at_seq).alias("min_moved_at_seq_sum_moved_at_seq"),
    )
)

result = (
    with_windows
    .filter(F.col("min_moved_at_seq_sum_moved_at_seq") > 41)
    .select(
        "moved_at_seq",
        "min_moved_at_seq",
        "total_unit_price_vs_sku_id_avg",
        "n_operator_is_not_null",
        "prev_moved_at_seq",
        "min_moved_at_seq_sum_moved_at_seq",
    )
    .orderBy(F.col("moved_at_seq").desc())
    .limit(10)
)
