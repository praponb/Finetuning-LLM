from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_moved_at_year_move_type_by_q = Window.partitionBy(F.year("moved_at"), "move_type").orderBy(F.col("qty").asc_nulls_last(), "id")
w_distinct_sku_id_in_moved_at = Window.partitionBy("distinct_sku_id_in_moved_at_to_date_warehouse_id")

base = (
    df_stock_moves
    .filter(F.col("qty") <= 426)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.first("qty").over(w_warehouse_id_by_qty_full).alias("first_qty"),
        F.size(F.collect_set("sku_id").over(w_moved_at_to_date_warehouse_i)).alias("distinct_sku_id_in_moved_at_to_date_warehouse_id"),
        F.lag("least_unit_price_qty").over(w_warehouse_id_by_qty).alias("prev_least_unit_price_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_to_date_warehouse_i), F.lit(0)), 4).alias("qty_share_of_moved_at_to_date_warehouse_id"),
        F.max("qty").over(w_moved_at_to_date_warehouse_i).alias("moved_at_to_date_warehouse_id_max_qty"),
        F.lead("least_unit_price_qty", 3).over(w_moved_at_year_move_type_by_q).alias("next3_least_unit_price_qty"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.avg("next3_least_unit_price_qty").over(w_distinct_sku_id_in_moved_at), 2).alias("distinct_sku_id_in_moved_at_to_date_warehouse_id_2"),
        F.round(F.col("next3_least_unit_price_qty") - F.avg("next3_least_unit_price_qty").over(w_distinct_sku_id_in_moved_at), 2).alias("next3_least_unit_price_qty_vs_distinct_sku_id_in"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count_2"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

without_matches = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isNotNull()), "left_anti")
)

result = (
    without_matches
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "least_unit_price_qty",
        "qty_share_of_moved_at_to_date_warehouse_id",
        "moved_at_to_date_warehouse_id_max_qty",
        "next3_least_unit_price_qty",
        "distinct_sku_id_in_moved_at_to_date_warehouse_id_2",
        "next3_least_unit_price_qty_vs_distinct_sku_id_in",
        "move_count",
        "move_count_2",
    )
    .orderBy(F.col("move_type").desc(), F.col("sku_id").asc_nulls_last())
)
