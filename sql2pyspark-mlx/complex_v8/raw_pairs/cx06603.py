from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_distinct_warehouse_id = Window.orderBy("distinct_warehouse_ids", "moved_at_plus_30m_month", "moved_at_month", "sku_id")
w_sku_id_moved_at_plus_30m_mon = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at_plus_30m_month")).orderBy("total_qty", "moved_at_plus_30m_month", "moved_at_month", "sku_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_warehouse_ids_sku_i = Window.partitionBy("distinct_warehouse_ids", "sku_id")
w_all_by_distinct_warehouse_id_2 = Window.orderBy("distinct_warehouse_ids", "moved_at_plus_30m_month", "moved_at_month", "sku_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_moves = (
    df_stock_moves
    .filter(F.col("qty").between(85, 271))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.abs(F.col("unit_price") - 236.5).alias("unit_price_distance"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

totals = (
    scoped_moves
    .groupBy(
        F.date_trunc("month", "moved_at_plus_30m").alias("moved_at_plus_30m_month"),
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "sku_id",
    )
    .agg(
        F.round(F.sum("unit_price_distance"), 2).alias("total_unit_price_distance"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.sum("qty").alias("total_qty"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

with_activity = (
    totals.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.moved_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")), "left_anti")
)

metrics = (
    with_activity
    .select(
        "*",
        F.dense_rank().over(w_all_by_distinct_warehouse_id).alias("distinct_warehouse_ids_dense_rank"),
        F.last("moved_at_plus_30m_month").over(w_sku_id_moved_at_plus_30m_mon).alias("last_moved_at_plus_30m_month"),
        F.min("total_unit_price_distance").over(w_distinct_warehouse_ids_sku_i).alias("distinct_warehouse_ids_sku_id_min_total_unit_pri"),
        F.first("total_qty").over(w_all_by_distinct_warehouse_id_2).alias("first_total_qty"),
        F.last("total_unit_price_distance").over(w_all_by_distinct_warehouse_id_2).alias("last_total_unit_price_distance"),
    )
)

scored = (
    metrics
    .filter(F.col("total_qty") < 1950)
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("moved_at_plus_30m_month")) - F.unix_timestamp(F.col("moved_at_month"))) / 60, 1).alias("minutes_moved_at_month_to_moved_at_plus_30m_mont"),
        (F.col("first_total_qty") - F.col("total_qty")).alias("first_total_qty_minus_total_qty"),
    )
)

with_details_sku = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.moved_at_plus_30m_month",
        "s.moved_at_month",
        "s.sku_id",
        "s.total_qty",
        "s.distinct_operators",
        "s.distinct_ids",
        "s.last_moved_at_plus_30m_month",
        "s.distinct_warehouse_ids_sku_id_min_total_unit_pri",
        "s.last_total_unit_price_distance",
        "s.minutes_moved_at_month_to_moved_at_plus_30m_mont",
        "s.first_total_qty_minus_total_qty",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

line_level = (
    with_details_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.moved_at_plus_30m_month",
        "s.moved_at_month",
        "s.sku_id",
        "s.total_qty",
        "s.distinct_ids",
        "s.last_moved_at_plus_30m_month",
        "s.distinct_warehouse_ids_sku_id_min_total_unit_pri",
        "s.last_total_unit_price_distance",
        "s.minutes_moved_at_month_to_moved_at_plus_30m_mont",
        "s.sku_code",
        "s.sku_weight_kg",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
    )
)

result = (
    line_level
    .select(
        "moved_at_plus_30m_month",
        "moved_at_month",
        "total_qty",
        "distinct_ids",
        "last_moved_at_plus_30m_month",
        "distinct_warehouse_ids_sku_id_min_total_unit_pri",
        "minutes_moved_at_month_to_moved_at_plus_30m_mont",
        "sku_code",
        "sku_weight_kg",
        "move_id",
        "move_moved_at",
    )
    .orderBy(F.col("sku_weight_kg").asc(), F.col("distinct_ids").desc())
)
