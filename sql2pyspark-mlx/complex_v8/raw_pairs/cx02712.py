from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_moving_3 = Window.orderBy("moved_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_moved_at = Window.orderBy("moved_at", "id")

base = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug_2"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.first("moved_at_plus_2h").over(w_all_by_moved_at_full).alias("first_moved_at_plus_2h"),
        F.round(F.avg("qty").over(w_all_by_moved_at_moving_3), 2).alias("qty_avg_4"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.rank().over(w_all_by_moved_at).alias("moved_at_rank"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.rank().over(w_all_by_moved_at_ties).alias("moved_at_rank_2"),
    )
)

leaders = (
    with_trends
    .filter(F.col("moved_at_rank_2") <= 3)
    .drop("moved_at_rank_2")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

summarized_rows = (
    leaders.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.first_moved_at",
        "st.last_moved_at_2",
    )
)

result = (
    summarized_rows
    .rollup("sku_id", "moved_at_rank", "operator")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("last_moved_at_2").alias("last_last_moved_at_2"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("moved_at_rank").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
