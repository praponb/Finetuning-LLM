from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy(F.col("unit_price_adj").desc(), F.col("id").desc())
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_moved_at_full = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_unit_price_adj = Window.partitionBy("move_type").orderBy(F.col("unit_price_adj").desc())
w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_by_moved_at_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest_2 = Window.partitionBy("operator").orderBy(F.col("running_running_qty").desc(), F.col("id").desc())
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_unit_price_adj_percen = Window.orderBy(F.col("unit_price_adj_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_running_qty = Window.orderBy("running_qty", "id")

cleaned = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") | (F.col("sku_id") <= 18))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

latest_per_key = (
    cleaned
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    latest_per_key
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price_adj",
        "operator_ref",
        F.round(F.col("unit_price_adj") / F.nullif(F.sum("unit_price_adj").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_adj_share_of_move_type_operator"),
        F.last("unit_price_adj", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_unit_price_adj"),
        F.last("qty").over(w_move_type_by_moved_at_full).alias("last_qty"),
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
        F.round(F.percent_rank().over(w_move_type_by_unit_price_adj), 3).alias("unit_price_adj_percent_rank"),
        F.sum("qty").over(w_move_type_by_moved_at_runnin).alias("running_qty"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.ntile(5).over(w_operator_by_moved_at).alias("moved_at_quintile"),
        F.sum("running_qty").over(w_operator_by_moved_at_running).alias("running_running_qty"),
    )
)

latest_per_key_2 = (
    metrics
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest_per_key_2
    .select(
        "id",
        "move_type",
        "operator",
        "unit_price_adj",
        "operator_ref",
        "last_known_unit_price_adj",
        "last_qty",
        "moved_at_quintile",
        "running_running_qty",
        F.sum("unit_price_adj").over(w_sku_id_move_type).alias("sku_id_move_type_sum_unit_price_adj"),
        F.nth_value("running_qty", 2).over(w_all_by_unit_price_adj_percen).alias("second_running_qty"),
        F.ntile(5).over(w_all_by_running_qty).alias("running_qty_quintile"),
        F.nth_value("moved_at_quintile", 2).over(w_all_by_unit_price_adj_percen).alias("second_moved_at_quintile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "move_type",
        "operator",
        "unit_price_adj",
        "operator_ref",
        "last_known_unit_price_adj",
        "last_qty",
        "moved_at_quintile",
        "running_running_qty",
        "second_running_qty",
    )
    .orderBy("last_qty", F.col("running_running_qty").asc())
)
