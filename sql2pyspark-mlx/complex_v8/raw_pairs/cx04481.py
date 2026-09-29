from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_2 = Window.partitionBy("warehouse_id")
w_all_by_unit_price_prior_6 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, -1)
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_starts_session_by_prev_moved = Window.partitionBy("starts_session").orderBy(F.col("prev_moved_at").desc(), "id")
w_all_by_qty_prior_6 = Window.orderBy("qty", "id").rowsBetween(-6, -1)

cleaned = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_warehouse_id_running))

with_windows = (
    sessionized
    .select(
        "*",
        F.round(F.col("starts_session") - F.avg("starts_session").over(w_warehouse_id_2), 2).alias("starts_session_vs_warehouse_id_avg"),
        F.sum("unit_price").over(w_all_by_unit_price_prior_6).alias("unit_price_sum_6_prior"),
        F.max("unit_price").over(w_all_by_unit_price_running).alias("max_unit_price_so_far"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_warehouse_id_2), F.lit(0)), 4).alias("session_no_share_of_warehouse_id"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_2)).alias("distinct_sku_id_in_warehouse_id"),
        F.round(F.avg("starts_session").over(w_all_by_unit_price_prior_6), 2).alias("starts_session_avg_6_prior"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "moved_at_date",
        "prev_moved_at",
        "starts_session",
        "session_no",
        "starts_session_vs_warehouse_id_avg",
        "unit_price_sum_6_prior",
        "max_unit_price_so_far",
        "session_no_share_of_warehouse_id",
        "distinct_sku_id_in_warehouse_id",
        "starts_session_avg_6_prior",
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.lead("unit_price", 1, 0).over(w_starts_session_by_prev_moved).alias("next_unit_price"),
        F.sum("max_unit_price_so_far").over(w_all_by_qty_prior_6).alias("max_unit_price_so_far_sum_6_prior"),
    )
)

result = (
    ranked
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at_date",
        "prev_moved_at",
        "starts_session",
        "session_no",
        "starts_session_vs_warehouse_id_avg",
        "session_no_share_of_warehouse_id",
        "starts_session_avg_6_prior",
        "distinct_warehouse_id_in_sku_id",
        "max_unit_price_so_far_sum_6_prior",
        F.round(F.col("max_unit_price_so_far") * 1.1, 2).alias("max_unit_price_so_far_adj"),
    )
    .orderBy(F.col("starts_session_vs_warehouse_id_avg").asc(), "session_no", "id")
    .limit(20)
)
