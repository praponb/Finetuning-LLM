from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_qty_moving_2 = Window.orderBy(F.col("last_qty").desc(), "id").rowsBetween(-2, Window.currentRow)
w_moved_at_dense_rank = Window.partitionBy("moved_at_dense_rank")
w_all_by_last_qty = Window.orderBy(F.col("last_qty").desc(), "id")
w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy("moved_at_cume_dist", "id")
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_move_type_by_moved_at_prior = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, -1)
w_all_by_moved_at_cume_dist = Window.orderBy(F.col("moved_at_cume_dist").desc(), "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_latest = Window.partitionBy("move_type").orderBy(F.col("moved_at_dense_rank_avg_first_known_unit_price").desc(), F.col("id").desc())

valid = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_dew", "op_ann"))
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
    valid
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.first("unit_price", ignorenulls=True).over(w_all_by_moved_at_running).alias("first_known_unit_price"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
        F.last("qty").over(w_all_by_moved_at_full).alias("last_qty"),
    )
)

leaders = sequenced.filter(F.col("moved_at_dense_rank") == 1)

with_trends = (
    leaders
    .select(
        "*",
        F.max("first_known_unit_price").over(w_all_by_last_qty_moving_2).alias("first_known_unit_price_max_3"),
        F.round(F.avg("first_known_unit_price").over(w_moved_at_dense_rank), 2).alias("moved_at_dense_rank_avg_first_known_unit_price"),
        F.round(F.col("last_qty") / F.nullif(F.sum("last_qty").over(w_moved_at_dense_rank), F.lit(0)), 4).alias("last_qty_share_of_moved_at_dense_rank"),
        F.ntile(4).over(w_all_by_last_qty).alias("last_qty_quartile"),
        (F.col("last_qty") - F.lag("last_qty").over(w_operator_move_type_by_moved)).alias("last_qty_change"),
        F.count(F.lit(1)).over(w_moved_at_dense_rank).alias("rows_in_moved_at_dense_rank"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "moved_at_dense_rank",
        "first_known_unit_price",
        "first_known_unit_price_max_3",
        "moved_at_dense_rank_avg_first_known_unit_price",
        "rows_in_moved_at_dense_rank",
        F.dense_rank().over(w_all_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.round(F.avg("unit_price").over(w_move_type_by_moved_at_prior), 2).alias("unit_price_avg_3_prior"),
        F.ntile(4).over(w_all_by_moved_at_cume_dist).alias("moved_at_cume_dist_quartile"),
        F.lead("moved_at").over(w_all_by_unit_price).alias("next_moved_at"),
    )
)

deduped = (
    with_trends_2
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

move_type_stats = (
    deduped
    .groupBy("move_type")
    .agg(F.avg("unit_price_avg_3_prior").alias("avg_unit_price_avg_3_prior"))
)

vs_group = (
    deduped.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price_avg_3_prior") > F.col("g.avg_unit_price_avg_3_prior"))
    .select(
        "s.*",
        "g.avg_unit_price_avg_3_prior",
        F.round(F.col("s.unit_price_avg_3_prior") - F.col("g.avg_unit_price_avg_3_prior"), 2).alias("unit_price_avg_3_prior_minus_avg"),
    )
)

result = (
    vs_group
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "moved_at_dense_rank",
        "first_known_unit_price_max_3",
        "moved_at_dense_rank_avg_first_known_unit_price",
        "rows_in_moved_at_dense_rank",
        "unit_price_avg_3_prior",
        "next_moved_at",
        "avg_unit_price_avg_3_prior",
        "unit_price_avg_3_prior_minus_avg",
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
    .orderBy(
        F.col("unit_price_avg_3_prior_minus_avg").desc(),
        F.col("qty").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
