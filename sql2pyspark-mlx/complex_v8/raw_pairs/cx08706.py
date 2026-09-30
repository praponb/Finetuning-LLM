from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_moved_at_to_date_b = Window.partitionBy("move_type", F.to_date("moved_at")).orderBy(F.col("moved_at").desc())
w_move_type_moved_at_to_date_b_2 = Window.partitionBy("move_type", F.to_date("moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_operator_moved_at_month = Window.partitionBy("operator", F.month("moved_at"))
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id")
w_move_type_moved_at_to_date_b_3 = Window.partitionBy("move_type", F.to_date("moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_moved_at_to_date_by_rows_in = Window.partitionBy(F.to_date("moved_at")).orderBy("rows_in_operator_moved_at_month", "id")
w_all_by_next_unit_price = Window.orderBy(F.col("next_unit_price").desc(), "id")
w_move_type = Window.partitionBy("move_type")

recent = (
    df_stock_moves
    .filter(F.col("unit_price") <= 396.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

with_details = (
    recent.alias("s")
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
        "s.moved_at_label",
        "s.move_type_ref",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_move_type_moved_at_to_date_b), 3).alias("moved_at_cume_dist"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_moved_at_to_date_b_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.size(F.collect_set("move_type").over(w_operator_moved_at_month)).alias("distinct_move_type_in_operator_moved_at_month"),
        F.lag("unit_price", 2).over(w_all_by_moved_at).alias("prev2_unit_price"),
        F.row_number().over(w_all_by_moved_at).alias("moved_at_seq"),
        F.lead("qty").over(w_move_type_moved_at_to_date_b_2).alias("next_qty"),
        F.row_number().over(w_operator_warehouse_id_by_uni).alias("unit_price_seq"),
        F.round(F.avg("unit_price").over(w_operator_moved_at_month), 2).alias("operator_moved_at_month_avg_unit_price"),
        F.round(F.avg("qty").over(w_move_type_moved_at_to_date_b_3), 2).alias("qty_avg_7"),
        F.count(F.lit(1)).over(w_operator_moved_at_month).alias("rows_in_operator_moved_at_month"),
        F.max("unit_price").over(w_operator_moved_at_month).alias("operator_moved_at_month_max_unit_price"),
        F.lead("unit_price").over(w_all_by_moved_at).alias("next_unit_price"),
    )
)

top_per_group = metrics.filter(F.col("moved_at_dense_rank") <= 3)

sequenced = (
    top_per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_moved_at_to_date_by_rows_in), 3).alias("rows_in_operator_moved_at_month_cume_dist"),
        F.lead("next_unit_price").over(w_all_by_next_unit_price).alias("next_next_unit_price"),
        F.round(F.col("minutes_since_prev_moved_at") / F.nullif(F.sum("minutes_since_prev_moved_at").over(w_move_type), F.lit(0)), 4).alias("minutes_since_prev_moved_at_share_of_move_type"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_ref",
        "warehouse_wh_code",
        "moved_at_cume_dist",
        "minutes_since_prev_moved_at",
        "moved_at_dense_rank",
        "distinct_move_type_in_operator_moved_at_month",
        "prev2_unit_price",
        "moved_at_seq",
        "next_qty",
        "unit_price_seq",
        "operator_moved_at_month_avg_unit_price",
        "qty_avg_7",
        "rows_in_operator_moved_at_month",
        "operator_moved_at_month_max_unit_price",
        "next_unit_price",
        "rows_in_operator_moved_at_month_cume_dist",
        "minutes_since_prev_moved_at_share_of_move_type",
        F.when(F.col("move_type").isin("ADJUST", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label_2"),
    )
    .orderBy(
        F.col("prev2_unit_price").desc(),
        F.col("moved_at_label_2").asc_nulls_last(),
    )
)
