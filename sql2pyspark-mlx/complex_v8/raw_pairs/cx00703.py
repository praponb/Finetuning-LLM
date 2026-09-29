from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_sku_id_move_type_by_unit_pri = Window.partitionBy("sku_id", "move_type").orderBy("unit_price_adj", "id")
w_move_type_group_sku_id_by_un = Window.partitionBy("move_type_group", "sku_id").orderBy("unit_price_adj", "id")
w_move_type_group_sku_id_by_un_2 = Window.partitionBy("move_type_group", "sku_id").orderBy("unit_price_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_qty_ties = Window.partitionBy("move_type").orderBy("qty")
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_median_qty_dense_rank_move_t = Window.partitionBy("median_qty_dense_rank", "move_type").orderBy(F.col("max_unit_price").desc(), "move_type_group", "move_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_group_n_move_type = Window.partitionBy("move_type_group", "n_move_type_adjust")

prepared_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

with_trends = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_group",
        "unit_price_adj",
        F.round(F.col("unit_price_adj") / F.nullif(F.sum("unit_price_adj").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_adj_share_of_warehouse_id"),
        (F.col("unit_price_adj") - F.lag("unit_price_adj").over(w_move_type_by_qty)).alias("unit_price_adj_change"),
        F.rank().over(w_sku_id_move_type_by_unit_pri).alias("unit_price_adj_rank"),
        F.rank().over(w_move_type_group_sku_id_by_un).alias("unit_price_adj_rank_2"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.lag("qty", 2).over(w_move_type_by_qty).alias("prev2_qty"),
        F.last("unit_price", ignorenulls=True).over(w_move_type_group_sku_id_by_un_2).alias("last_known_unit_price"),
        F.dense_rank().over(w_move_type_by_qty_ties).alias("qty_dense_rank"),
        F.row_number().over(w_move_type_by_qty).alias("seq_no"),
        F.round(F.col("unit_price_adj") - F.avg("unit_price_adj").over(w_warehouse_id), 2).alias("unit_price_adj_vs_warehouse_id_avg"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.ntile(10).over(w_move_type_by_qty).alias("qty_decile"),
        F.nth_value("unit_price", 2).over(w_move_type_by_qty_full).alias("second_unit_price"),
    )
)

top_rows = with_trends.filter(F.col("seq_no") <= 2)

totals = (
    top_rows
    .groupBy("move_type_group", "move_type")
    .agg(
        F.count(F.when(F.col("move_type") == "ADJUST", 1)).alias("n_move_type_adjust"),
        F.max("unit_price").alias("max_unit_price"),
        F.percentile_approx("qty_dense_rank", 0.5).alias("median_qty_dense_rank"),
    )
)

with_trends_2 = (
    totals
    .select(
        "*",
        F.first("max_unit_price").over(w_median_qty_dense_rank_move_t).alias("first_max_unit_price"),
        F.round(F.avg("n_move_type_adjust").over(w_move_type_group_n_move_type), 2).alias("move_type_group_n_move_type_adjust_avg_n_move_ty"),
        F.first("n_move_type_adjust").over(w_median_qty_dense_rank_move_t).alias("first_n_move_type_adjust"),
        F.count(F.lit(1)).over(w_move_type_group_n_move_type).alias("rows_in_move_type_group_n_move_type_adjust"),
    )
)

result = (
    with_trends_2
    .filter(F.col("move_type").isin("TRANSFER", "OUTBOUND"))
    .select(
        "move_type_group",
        "move_type",
        "median_qty_dense_rank",
        "move_type_group_n_move_type_adjust_avg_n_move_ty",
        "first_n_move_type_adjust",
        F.when(F.col("move_type_group").isin("group_b", "other"), "group_a").when(F.col("move_type_group") == "group_a", "group_b").otherwise("other").alias("move_type_group_group"),
    )
    .orderBy(F.col("move_type_group").asc())
)
