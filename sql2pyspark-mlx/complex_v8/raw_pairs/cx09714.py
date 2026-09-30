from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator = Window.partitionBy("move_type", "operator")
w_move_type_moved_at_to_date = Window.partitionBy("move_type", F.to_date("moved_at"))
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_moved_at_month_2 = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_all_by_distinct_operator_in = Window.orderBy(F.col("distinct_operator_in_move_type_moved_at_to_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_distinct_operat = Window.partitionBy("move_type").orderBy(F.col("distinct_operator_in_move_type_moved_at_to_date").desc(), "id")
w_all_by_distinct_operator_in_2 = Window.orderBy(F.col("distinct_operator_in_move_type_moved_at_to_date").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_move_type_by_distinct_operat_2 = Window.partitionBy("move_type").orderBy(F.col("distinct_operator_in_move_type_moved_at_to_date").desc(), "id").rowsBetween(-3, -1)

cleaned = (
    df_stock_moves
    .filter((F.col("operator") == "op_eve") | (F.col("move_type").isNotNull()))
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

sequenced = (
    cleaned
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_share_of_move_type_operator"),
        F.size(F.collect_set("operator").over(w_move_type_moved_at_to_date)).alias("distinct_operator_in_move_type_moved_at_to_date"),
        F.lead("unit_price", 2).over(w_warehouse_id_moved_at_month).alias("next2_unit_price"),
        F.row_number().over(w_move_type_by_moved_at).alias("moved_at_seq"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type_operator), F.lit(0)), 4).alias("qty_share_of_move_type_operator"),
        F.round(F.cume_dist().over(w_warehouse_id_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.sum("qty").over(w_warehouse_id_moved_at_month_2).alias("running_qty"),
        (F.col("qty") - F.lag("qty").over(w_move_type_by_moved_at)).alias("qty_change"),
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price_share_of_move_type_operator",
        "distinct_operator_in_move_type_moved_at_to_date",
        "next2_unit_price",
        "moved_at_seq",
        "qty_share_of_move_type_operator",
        "unit_price_cume_dist",
        "running_qty",
        "qty_change",
        "distinct_sku_id_in_move_type_operator",
        F.abs(F.col("next2_unit_price") - 273.5).alias("next2_unit_price_distance"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price_share_of_move_type_operator",
        "next2_unit_price",
        "moved_at_seq",
        "unit_price_cume_dist",
        "distinct_sku_id_in_move_type_operator",
        "move_type_ref",
        "days_since_moved_at",
        F.min("next2_unit_price_distance").over(w_move_type_sku_id).alias("move_type_sku_id_min_next2_unit_price_distance"),
        F.count(F.lit(1)).over(w_move_type_sku_id).alias("rows_in_move_type_sku_id"),
        F.first("days_since_moved_at").over(w_all_by_distinct_operator_in).alias("first_days_since_moved_at"),
        F.round(F.percent_rank().over(w_move_type_by_distinct_operat), 3).alias("distinct_operator_in_move_type_moved_at_to_date_"),
        F.round(F.avg("next2_unit_price_distance").over(w_all_by_distinct_operator_in_2), 2).alias("next2_unit_price_distance_avg_3"),
        F.max("unit_price_cume_dist").over(w_move_type_by_distinct_operat_2).alias("unit_price_cume_dist_max_3_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price_share_of_move_type_operator",
        "next2_unit_price",
        "moved_at_seq",
        "distinct_sku_id_in_move_type_operator",
        "days_since_moved_at",
        "move_type_sku_id_min_next2_unit_price_distance",
        "rows_in_move_type_sku_id",
        "distinct_operator_in_move_type_moved_at_to_date_",
        "unit_price_cume_dist_max_3_prior",
    )
    .orderBy(
        F.col("move_type_sku_id_min_next2_unit_price_distance").desc(),
        F.col("unit_price_cume_dist_max_3_prior").desc(),
    )
)
