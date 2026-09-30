from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_sku_category_by_unit_price = Window.partitionBy("sku_category").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_opened_on_month_op = Window.partitionBy(F.month("warehouse_opened_on"), "operator").orderBy(F.col("warehouse_opened_on").asc_nulls_last())
w_sku_category_by_unit_price_f = Window.partitionBy("sku_category").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type_group = Window.partitionBy("warehouse_id", "move_type_group")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_opened_on_month_op_2 = Window.partitionBy(F.month("warehouse_opened_on"), "operator").orderBy(F.col("warehouse_opened_on").asc_nulls_last(), "id")
w_sku_category_by_unit_price_r = Window.partitionBy("sku_category").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_opened_on_dense_ra = Window.partitionBy("warehouse_opened_on_dense_rank_2").orderBy(F.col("unit_price_share_of_operator_warehouse_id").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_opened_on_dense_ra_2 = Window.partitionBy("warehouse_opened_on_dense_rank_2").orderBy(F.col("unit_price_share_of_operator_warehouse_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_tercile = Window.partitionBy("unit_price_tercile")

filtered = (
    df_stock_moves
    .filter((F.col("operator").isin("op_ann", "op_eve", "op_dew")) & (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

joined_sku = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_group",
        F.col("sk.category").alias("sku_category"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

metrics = (
    joined_sku
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator_warehouse_id), 2).alias("unit_price_vs_operator_warehouse_id_avg"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator_warehouse_id), 2).alias("qty_vs_operator_warehouse_id_avg"),
        F.max("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_max_unit_price"),
        F.ntile(5).over(w_sku_category_by_unit_price).alias("unit_price_quintile"),
        F.dense_rank().over(w_warehouse_opened_on_month_op).alias("warehouse_opened_on_dense_rank"),
        F.dense_rank().over(w_sku_category_by_unit_price).alias("unit_price_dense_rank"),
        F.nth_value("warehouse_opened_on", 2).over(w_sku_category_by_unit_price_f).alias("second_warehouse_opened_on"),
        F.sum("qty").over(w_warehouse_id_move_type_group).alias("warehouse_id_move_type_group_sum_qty"),
        F.ntile(3).over(w_sku_id_by_unit_price).alias("unit_price_tercile"),
        F.size(F.collect_set("sku_category").over(w_operator_warehouse_id)).alias("distinct_sku_category_in_operator_warehouse_id"),
        F.sum("unit_price").over(w_warehouse_id_move_type_group).alias("warehouse_id_move_type_group_sum_unit_price"),
        F.dense_rank().over(w_warehouse_opened_on_month_op_2).alias("warehouse_opened_on_dense_rank_2"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type_group).alias("rows_in_warehouse_id_move_type_group"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_warehouse_id), F.lit(0)), 4).alias("qty_share_of_operator_warehouse_id"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_sku_category_by_unit_price_r).alias("last_known_warehouse_opened_on"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_operator_warehouse_id"),
    )
)

leaders = (
    metrics
    .filter(F.col("unit_price_dense_rank") <= 5)
    .drop("unit_price_dense_rank")
)

with_trends = (
    leaders
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_warehouse_opened_on_dense_ra), 2).alias("unit_price_avg_2"),
        F.first("unit_price_tercile", ignorenulls=True).over(w_warehouse_opened_on_dense_ra_2).alias("first_known_unit_price_tercile"),
        F.round(F.avg("unit_price_quintile").over(w_unit_price_tercile), 2).alias("unit_price_tercile_avg_unit_price_quintile"),
    )
)

result = (
    with_trends
    .filter(F.col("first_known_unit_price_tercile") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "move_type_group",
        "warehouse_opened_on",
        "qty_vs_operator_warehouse_id_avg",
        "operator_warehouse_id_max_unit_price",
        "unit_price_quintile",
        "warehouse_opened_on_dense_rank",
        "second_warehouse_opened_on",
        "warehouse_id_move_type_group_sum_qty",
        "unit_price_tercile",
        "warehouse_id_move_type_group_sum_unit_price",
        "warehouse_opened_on_dense_rank_2",
        "rows_in_warehouse_id_move_type_group",
        "qty_share_of_operator_warehouse_id",
        "last_known_warehouse_opened_on",
        "unit_price_avg_2",
        "first_known_unit_price_tercile",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
    .orderBy("id", F.col("rows_in_warehouse_id_move_type_group").asc())
)
