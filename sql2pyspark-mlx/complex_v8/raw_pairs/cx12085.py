from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_group_operator_by = Window.partitionBy("move_type_group", "operator").orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id")
w_operator_by_unit_price_movin = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_by_unit_price_f = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price").desc(), "id")
w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id")
w_operator = Window.partitionBy("operator")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_by_sku_weight_k = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_days_since_moved_at_s = Window.orderBy("days_since_moved_at_share_of_sku_id", "id").rowsBetween(-3, -1)
w_move_type_group_by_operator = Window.partitionBy("move_type_group").orderBy(F.col("operator_max_days_since_moved_at").desc(), "id")
w_warehouse_id_by_sku_weight_k_2 = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg_tercile").desc(), "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
    )
)

with_details = (
    recent_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.move_type_group",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.ntile(3).over(w_move_type_group_operator_by).alias("sku_weight_kg_tercile"),
        F.sum("days_since_moved_at").over(w_operator_by_unit_price_movin).alias("days_since_moved_at_sum_2"),
        F.last("days_since_moved_at").over(w_warehouse_id_by_unit_price_f).alias("last_days_since_moved_at"),
        F.dense_rank().over(w_operator_sku_id_by_unit_pric).alias("unit_price_dense_rank"),
        F.lag("days_since_moved_at", 3, 0).over(w_operator_by_unit_price).alias("prev3_days_since_moved_at"),
        F.max("days_since_moved_at").over(w_operator).alias("operator_max_days_since_moved_at"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_sku_id), F.lit(0)), 4).alias("days_since_moved_at_share_of_sku_id"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.last("sku_weight_kg").over(w_warehouse_id_by_sku_weight_k).alias("last_sku_weight_kg"),
        F.min("days_since_moved_at").over(w_operator_warehouse_id).alias("operator_warehouse_id_min_days_since_moved_at"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_sku_id), F.lit(0)), 4).alias("days_since_moved_at_share_of_sku_id_2"),
        F.sum("last_days_since_moved_at").over(w_all_by_days_since_moved_at_s).alias("last_days_since_moved_at_sum_3_prior"),
        F.round(F.col("unit_price_dense_rank") - F.avg("unit_price_dense_rank").over(w_sku_id), 2).alias("unit_price_dense_rank_vs_sku_id_avg"),
        F.round(F.percent_rank().over(w_move_type_group_by_operator), 3).alias("operator_max_days_since_moved_at_percent_rank"),
        F.row_number().over(w_warehouse_id_by_sku_weight_k_2).alias("rn"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "days_since_moved_at",
        "move_type_group",
        "warehouse_wh_code",
        "sku_labels",
        "sku_weight_kg_tercile",
        "days_since_moved_at_sum_2",
        "last_days_since_moved_at",
        "unit_price_dense_rank",
        "operator_max_days_since_moved_at",
        "distinct_warehouse_id_in_sku_id",
        "operator_warehouse_id_min_days_since_moved_at",
        "days_since_moved_at_share_of_sku_id_2",
        "last_days_since_moved_at_sum_3_prior",
        "unit_price_dense_rank_vs_sku_id_avg",
        "operator_max_days_since_moved_at_percent_rank",
        "rn",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
        F.coalesce(F.col("sku_weight_kg"), F.lit(0.0)).alias("sku_weight_kg_filled"),
    )
    .orderBy("unit_price_dense_rank_vs_sku_id_avg", "id")
    .limit(5)
)
