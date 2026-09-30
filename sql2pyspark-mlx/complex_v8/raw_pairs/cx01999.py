from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_move_type_by_m = Window.partitionBy("move_operator", "move_type").orderBy(F.col("move_qty_minus_median").desc(), "id", "move_id")
w_opened_on_month_by_median_mo = Window.partitionBy(F.month("opened_on")).orderBy(F.col("median_move_qty").desc(), "id", "move_id")

base = (
    df_warehouses
    .filter(F.col("capacity") <= 11005)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_trunc("year", F.col("opened_on")).alias("opened_on_year"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.concat_ws(" / ", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

with_children_moves = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.opened_on",
        "s.opened_on_year",
        "s.capacity_bucket",
        "s.city_wh_code_label",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

move_operator_stats = (
    with_children_moves
    .groupBy("move_operator")
    .agg(F.percentile_approx("move_qty", 0.5).alias("median_move_qty"))
)

compared = (
    with_children_moves.alias("s")
    .join(move_operator_stats.alias("g"), F.col("g.move_operator") == F.col("s.move_operator"), "inner")
    .filter(F.col("s.move_qty") >= F.col("g.median_move_qty"))
    .select(
        "s.*",
        "g.median_move_qty",
        F.round(F.col("s.move_qty") - F.col("g.median_move_qty"), 2).alias("move_qty_minus_median"),
    )
)

windowed = (
    compared
    .select(
        "id",
        "opened_on_year",
        "capacity_bucket",
        "city_wh_code_label",
        "move_id",
        "move_type",
        "move_moved_at",
        "median_move_qty",
        "move_qty_minus_median",
        F.dense_rank().over(w_move_operator_move_type_by_m).alias("move_qty_minus_median_dense_rank"),
        F.round((F.unix_timestamp("opened_on_year") - F.unix_timestamp(F.lag("opened_on_year").over(w_opened_on_month_by_median_mo))) / 60, 1).alias("minutes_since_prev_opened_on_year"),
    )
)

result = (
    windowed
    .cube("move_id", "move_type", "move_qty_minus_median_dense_rank")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("capacity_bucket") / F.nullif(F.sum("move_qty_minus_median_dense_rank"), F.lit(0)), 3).alias("capacity_bucket_to_move_qty_minus_median_dense_r"),
        F.max("capacity_bucket").alias("max_capacity_bucket"),
    )
    .orderBy(
        F.col("move_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
        F.col("move_qty_minus_median_dense_rank").asc_nulls_last(),
    )
)
