from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_count_by_move = Window.partitionBy("move_operator_count").orderBy(F.col("move_operator_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_dayofweek = Window.partitionBy("moved_at_dayofweek")
w_all_by_moved_at_dayofweek = Window.orderBy(F.col("moved_at_dayofweek").desc(), "id")
w_all_by_moved_at_dayofweek_ti = Window.orderBy(F.col("moved_at_dayofweek").desc())

cleaned = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.operator").isin("op_bee", "op_ann", "op_dew")), "left_anti")
    .filter(F.col("move_type") == "OUTBOUND")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator").isin("op_ann", "op_eve", "op_chan"), 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

windowed = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "moved_at_dayofweek",
        F.first("moved_at_dayofweek", ignorenulls=True).over(w_move_operator_count_by_move).alias("first_known_moved_at_dayofweek"),
        F.size(F.collect_set("sku_id").over(w_moved_at_dayofweek)).alias("distinct_sku_id_in_moved_at_dayofweek"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_moved_at_dayofweek))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("greatest_unit_price_qty") / F.nullif(F.sum("greatest_unit_price_qty").over(w_moved_at_dayofweek), F.lit(0)), 4).alias("greatest_unit_price_qty_share_of_moved_at_dayofw"),
        F.round(F.cume_dist().over(w_all_by_moved_at_dayofweek_ti), 3).alias("moved_at_dayofweek_cume_dist"),
    )
)

move_type_stats = (
    windowed
    .groupBy("move_type")
    .agg(F.avg("minutes_since_prev_moved_at").alias("avg_minutes_since_prev_moved_at"))
)

outliers = (
    windowed.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.minutes_since_prev_moved_at") > F.col("g.avg_minutes_since_prev_moved_at"))
    .select(
        "s.*",
        "g.avg_minutes_since_prev_moved_at",
        F.round(F.col("s.minutes_since_prev_moved_at") - F.col("g.avg_minutes_since_prev_moved_at"), 2).alias("minutes_since_prev_moved_at_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "move_type",
        "moved_at",
        "moved_at_dayofweek",
        "first_known_moved_at_dayofweek",
        "distinct_sku_id_in_moved_at_dayofweek",
        "minutes_since_prev_moved_at",
        "greatest_unit_price_qty_share_of_moved_at_dayofw",
        "minutes_since_prev_moved_at_minus_avg",
    )
    .orderBy("greatest_unit_price_qty_share_of_moved_at_dayofw", "id")
    .limit(5)
)
