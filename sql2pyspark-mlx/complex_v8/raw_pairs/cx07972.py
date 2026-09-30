from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_sku_unit_cost = Window.partitionBy("sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first(), "id")
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_to_date_move_type_g = Window.partitionBy(F.to_date("moved_at"), "move_type_group")
w_warehouse_id_by_moved_at_pri = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_unit_price_dense_rank = Window.partitionBy("unit_price_dense_rank")
w_all_by_first_qty_running = Window.orderBy(F.col("first_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at_quartile = Window.partitionBy("sku_id").orderBy("moved_at_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
)

enriched = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_group",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_group",
        "sku_code",
        "sku_is_active",
        F.lag("sku_unit_cost", 3).over(w_all_by_unit_price).alias("prev3_sku_unit_cost"),
        F.row_number().over(w_warehouse_id_by_moved_at).alias("moved_at_seq"),
        F.round(F.avg("qty").over(w_warehouse_id), 2).alias("warehouse_id_avg_qty"),
        F.round(F.cume_dist().over(w_sku_id_by_sku_unit_cost), 3).alias("sku_unit_cost_cume_dist"),
        F.ntile(4).over(w_warehouse_id_by_moved_at).alias("moved_at_quartile"),
        F.last("qty").over(w_all_by_unit_price_full).alias("last_qty"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_moved_at_to_date_move_type_g), F.lit(0)), 4).alias("sku_unit_cost_share_of_moved_at_to_date_move_typ"),
        F.round(F.avg("sku_unit_cost").over(w_moved_at_to_date_move_type_g), 2).alias("moved_at_to_date_move_type_group_avg_sku_unit_co"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_moved_at_to_date_move_type_g), 2).alias("sku_unit_cost_vs_moved_at_to_date_move_type_grou"),
        F.round(F.avg("sku_unit_cost").over(w_warehouse_id_by_moved_at_pri), 2).alias("sku_unit_cost_avg_2_prior"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.first("qty").over(w_all_by_qty_full).alias("first_qty"),
        F.count(F.lit(1)).over(w_moved_at_to_date_move_type_g).alias("rows_in_moved_at_to_date_move_type_group"),
        (F.col("qty") - F.lag("qty").over(w_warehouse_id_by_moved_at)).alias("qty_change"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_unit_price)).alias("unit_price_change"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "move_type_group",
        "sku_is_active",
        "prev3_sku_unit_cost",
        "moved_at_seq",
        "moved_at_quartile",
        "sku_unit_cost_share_of_moved_at_to_date_move_typ",
        "moved_at_to_date_move_type_group_avg_sku_unit_co",
        "unit_price_dense_rank",
        "sku_unit_cost_vs_moved_at_to_date_move_type_grou",
        "unit_price_rank",
        "rows_in_moved_at_to_date_move_type_group",
        "unit_price_change",
        F.count(F.lit(1)).over(w_unit_price_dense_rank).alias("rows_in_unit_price_dense_rank"),
        F.first("unit_price_dense_rank", ignorenulls=True).over(w_all_by_first_qty_running).alias("first_known_unit_price_dense_rank"),
        F.first("unit_price_change").over(w_sku_id_by_moved_at_quartile).alias("first_unit_price_change"),
        F.size(F.collect_set("sku_id").over(w_unit_price_dense_rank)).alias("distinct_sku_id_in_unit_price_dense_rank"),
    )
)

result = (
    windowed
    .filter(F.col("first_unit_price_change") < 150.0)
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "sku_is_active",
        "moved_at_quartile",
        "moved_at_to_date_move_type_group_avg_sku_unit_co",
        "rows_in_moved_at_to_date_move_type_group",
        "unit_price_change",
        "rows_in_unit_price_dense_rank",
        "first_known_unit_price_dense_rank",
        "first_unit_price_change",
        "distinct_sku_id_in_unit_price_dense_rank",
        F.lower(F.trim(F.col("move_type_group"))).alias("move_type_group_clean"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
    .orderBy("moved_at", F.col("moved_at_hour").desc(), "id")
    .limit(20)
)
