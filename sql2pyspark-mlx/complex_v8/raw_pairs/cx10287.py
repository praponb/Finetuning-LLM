from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price_dista = Window.partitionBy("operator").orderBy("unit_price_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_by_moved_at = Window.partitionBy("operator").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at"))
w_all_by_unit_price_distance = Window.orderBy(F.col("unit_price_distance").desc(), "id")
w_operator_move_type_by_distin = Window.partitionBy("operator", "move_type").orderBy(F.col("distinct_warehouse_id_in_sku_id_moved_at_month").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_unit_price_dist = Window.partitionBy("warehouse_id", "unit_price_distance_quartile").orderBy("moved_at", "id")
w_operator = Window.partitionBy("operator")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.abs(F.col("unit_price") - 236.5).alias("unit_price_distance"),
    )
)

with_windows = (
    base_moves
    .select(
        "*",
        F.last("moved_at", ignorenulls=True).over(w_operator_by_unit_price_dista).alias("last_known_moved_at"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.lead("unit_price_distance", 2).over(w_operator_by_moved_at).alias("next2_unit_price_distance"),
        F.round(F.col("unit_price_distance") / F.nullif(F.sum("unit_price_distance").over(w_sku_id_moved_at_month), F.lit(0)), 4).alias("unit_price_distance_share_of_sku_id_moved_at_mon"),
        F.min("unit_price").over(w_warehouse_id).alias("warehouse_id_min_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_by_moved_at)).alias("unit_price_change"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_moved_at_month)).alias("distinct_warehouse_id_in_sku_id_moved_at_month"),
        F.ntile(4).over(w_all_by_unit_price_distance).alias("unit_price_distance_quartile"),
    )
)

tagged = (
    with_windows
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "operator_move_type_label",
        "unit_price_distance",
        "last_known_moved_at",
        "rows_in_warehouse_id",
        "unit_price_distance_share_of_sku_id_moved_at_mon",
        "unit_price_change",
        "distinct_warehouse_id_in_sku_id_moved_at_month",
        "unit_price_distance_quartile",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("operator_move_type_label")), " ", "_")).alias("operator_move_type_label_slug"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.sum("unit_price").over(w_operator_move_type_by_distin).alias("running_unit_price"),
        (F.col("unit_price_distance") - F.lag("unit_price_distance").over(w_warehouse_id_unit_price_dist)).alias("unit_price_distance_change"),
        F.round(F.col("unit_price_change") - F.avg("unit_price_change").over(w_operator), 2).alias("unit_price_change_vs_operator_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator",
        "unit_price",
        "operator_move_type_label",
        "unit_price_distance",
        "rows_in_warehouse_id",
        "unit_price_distance_share_of_sku_id_moved_at_mon",
        "distinct_warehouse_id_in_sku_id_moved_at_month",
        "unit_price_distance_quartile",
        "move_type_prefix",
        "running_unit_price",
        "unit_price_distance_change",
    )
    .orderBy(F.col("unit_price_distance_quartile").desc(), "id")
    .limit(20)
)
