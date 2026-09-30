from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_qty_p = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id").rowsBetween(-2, -1)
w_warehouse_id_move_type_by_qt = Window.partitionBy("warehouse_id", "move_type").orderBy("qty", "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_moved_at_day = Window.orderBy("moved_at_day", "id")
w_warehouse_id_by_qty_vs_sku_i = Window.partitionBy("warehouse_id").orderBy(F.col("qty_vs_sku_id_avg").desc())
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_distinct_move_type_in_sku_id = Window.partitionBy("distinct_move_type_in_sku_id_warehouse_id", F.year("moved_at_day")).orderBy("next2_moved_at_day", "id")
w_distinct_move_type_in_sku_id_2 = Window.partitionBy("distinct_move_type_in_sku_id").orderBy("qty_vs_sku_id_avg_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_move_type_in_sku_id_3 = Window.partitionBy("distinct_move_type_in_sku_id").orderBy("qty_vs_sku_id_avg_cume_dist", "id").rowsBetween(-2, -1)

scoped_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

with_trends = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_minus_qty",
        "moved_at_day",
        F.sum("qty").over(w_sku_id_warehouse_id_by_qty_p).alias("qty_sum_2_prior"),
        F.ntile(10).over(w_warehouse_id_move_type_by_qt).alias("qty_decile"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.lead("moved_at_day", 2).over(w_warehouse_id_move_type_by_qt).alias("next2_moved_at_day"),
    )
)

enriched = (
    with_trends
    .select(
        "*",
        F.abs(F.col("unit_price_minus_qty") - 650.4).alias("unit_price_minus_qty_distance"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

metrics = (
    enriched_2
    .select(
        "*",
        F.round((F.unix_timestamp("next2_moved_at_day") - F.unix_timestamp(F.lag("next2_moved_at_day").over(w_all_by_moved_at_day))) / 60, 1).alias("minutes_since_prev_next2_moved_at_day"),
        (F.col("qty_decile") - F.lag("qty_decile").over(w_all_by_moved_at_day)).alias("qty_decile_change"),
        F.round(F.cume_dist().over(w_warehouse_id_by_qty_vs_sku_i), 3).alias("qty_vs_sku_id_avg_cume_dist"),
        F.size(F.collect_set("move_type").over(w_sku_id_warehouse_id)).alias("distinct_move_type_in_sku_id_warehouse_id"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        (F.col("qty_vs_sku_id_avg_cume_dist") - F.lag("qty_vs_sku_id_avg_cume_dist").over(w_distinct_move_type_in_sku_id)).alias("qty_vs_sku_id_avg_cume_dist_change"),
        F.max("unit_price_minus_qty").over(w_distinct_move_type_in_sku_id_2).alias("max_unit_price_minus_qty_so_far"),
        F.round(F.percent_rank().over(w_distinct_move_type_in_sku_id), 3).alias("next2_moved_at_day_percent_rank"),
        F.sum("qty_sum_2_prior").over(w_distinct_move_type_in_sku_id_3).alias("qty_sum_2_prior_sum_2_prior"),
    )
)

result = (
    sequenced
    .groupBy("moved_at_day")
    .pivot("move_type", ["TRANSFER", "INBOUND", "OUTBOUND", "ADJUST"])
    .agg(F.max("qty"))
    .orderBy("moved_at_day")
)
