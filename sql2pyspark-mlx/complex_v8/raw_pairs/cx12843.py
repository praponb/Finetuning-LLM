from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_moved_at_month_by = Window.partitionBy("move_type", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_moved_at_month_by_2 = Window.partitionBy("move_type", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_move_type_moved_at_month_by_3 = Window.partitionBy("move_type", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_moved_at_to_date_by_s = Window.partitionBy("sku_id", F.to_date("moved_at")).orderBy("sku_weight_kg", "id").rowsBetween(-3, Window.currentRow)
w_sku_is_active_moved_at_month = Window.partitionBy("sku_is_active", F.month("moved_at")).orderBy("unit_price", "id")
w_sku_is_active_sku_id = Window.partitionBy("sku_is_active", "sku_id")
w_sku_is_active = Window.partitionBy("sku_is_active")
w_all_by_moved_at_moving_1 = Window.orderBy("moved_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_moved_at_moving_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")

base_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_chan") | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

enriched_sku = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_trends = (
    enriched_sku
    .select(
        "*",
        F.nth_value("sku_weight_kg", 2).over(w_move_type_moved_at_month_by).alias("second_sku_weight_kg"),
        F.row_number().over(w_move_type_moved_at_month_by_2).alias("rn"),
        F.min("unit_price").over(w_move_type_moved_at_month_by_3).alias("min_unit_price_so_far"),
        F.round(F.avg("sku_weight_kg").over(w_sku_id_moved_at_to_date_by_s), 2).alias("sku_weight_kg_avg_4"),
        F.ntile(4).over(w_sku_is_active_moved_at_month).alias("unit_price_quartile"),
        F.size(F.collect_set("move_type").over(w_sku_is_active_sku_id)).alias("distinct_move_type_in_sku_is_active_sku_id"),
        F.min("unit_price").over(w_sku_is_active).alias("sku_is_active_min_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_sku_is_active)).alias("distinct_warehouse_id_in_sku_is_active"),
        F.round(F.avg("sku_weight_kg").over(w_all_by_moved_at_moving_1), 2).alias("sku_weight_kg_avg_2"),
        F.sum("sku_weight_kg").over(w_all_by_moved_at_moving_2).alias("sku_weight_kg_sum_3"),
        F.lag("moved_at").over(w_all_by_moved_at).alias("prev_moved_at"),
    )
)

annotated = (
    with_trends
    .filter(F.col("sku_is_active_min_unit_price") > 2250.6)
    .select(
        "*",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.when(F.col("move_type").isin("OUTBOUND", "INBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.round((F.unix_timestamp(F.col("prev_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 60, 1).alias("minutes_moved_at_to_prev_moved_at"),
    )
)

result = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "sku_labels",
        "sku_weight_kg",
        "sku_is_active",
        "second_sku_weight_kg",
        "min_unit_price_so_far",
        "sku_weight_kg_avg_4",
        "unit_price_quartile",
        "distinct_move_type_in_sku_is_active_sku_id",
        "sku_is_active_min_unit_price",
        "distinct_warehouse_id_in_sku_is_active",
        "sku_weight_kg_avg_2",
        "sku_weight_kg_sum_3",
        "prev_moved_at",
        "move_type_or_unknown",
        "minutes_moved_at_to_prev_moved_at",
        F.concat(F.col("move_type_group"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_group_ref"),
        F.array_join(F.array_sort(F.col("sku_labels")), ",").alias("sku_labels_text"),
    )
    .orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id")
    .limit(10)
)
