from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_move_type_by_unit_price_full = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id_by_ope = Window.partitionBy("operator", "warehouse_id").orderBy("operator_len", "id").rowsBetween(-2, -1)
w_move_type_by_unit_price_movi = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_operator_warehouse_id_by_ope_2 = Window.partitionBy("operator", "warehouse_id").orderBy("operator_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty = Window.orderBy("qty", "id")
w_all_by_qty_prior_2 = Window.orderBy("qty", "id").rowsBetween(-2, -1)

filtered_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")) & (F.col("operator").isin("op_chan", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

windowed = (
    filtered_moves
    .select(
        "*",
        F.min("operator_len").over(w_move_type).alias("move_type_min_operator_len"),
        F.last("qty").over(w_move_type_by_unit_price_full).alias("last_qty"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_move_type), 2).alias("operator_len_vs_move_type_avg"),
        F.round(F.avg("qty").over(w_operator_warehouse_id_by_ope), 2).alias("qty_avg_2_prior"),
        F.round(F.avg("unit_price").over(w_move_type_by_unit_price_movi), 2).alias("unit_price_avg_7"),
        F.nth_value("qty", 2).over(w_operator_warehouse_id_by_ope_2).alias("second_qty"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.first("unit_price_avg_7", ignorenulls=True).over(w_all_by_qty_running).alias("first_known_unit_price_avg_7"),
        F.first("second_qty", ignorenulls=True).over(w_all_by_qty_running).alias("first_known_second_qty"),
        F.ntile(3).over(w_all_by_qty).alias("qty_tercile"),
        F.round(F.avg("qty_avg_2_prior").over(w_all_by_qty_prior_2), 2).alias("qty_avg_2_prior_avg_2_prior"),
    )
)

line_level = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_min_operator_len",
        "s.first_known_unit_price_avg_7",
        "s.first_known_second_qty",
        "s.qty_avg_2_prior_avg_2_prior",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    line_level
    .cube("move_type", "sku_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
