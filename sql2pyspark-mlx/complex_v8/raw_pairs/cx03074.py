from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_median_qty = Window.partitionBy("sku_id").orderBy(F.col("median_qty").desc_nulls_first(), "id")
w_all_by_qty_minus_median = Window.orderBy(F.col("qty_minus_median").desc(), "id")
w_sku_id_by_median_qty_running = Window.partitionBy("sku_id").orderBy(F.col("median_qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_minus_median_ties = Window.orderBy(F.col("qty_minus_median").desc())
w_move_type = Window.partitionBy("move_type")
w_operator_sku_id_by_qty_minus = Window.partitionBy("operator", "sku_id").orderBy("qty_minus_median", "id")
w_operator_sku_id_by_qty_minus_2 = Window.partitionBy("operator", "sku_id").orderBy("qty_minus_median")
w_all_by_median_qty = Window.orderBy(F.col("median_qty").desc(), "id")

base_moves = df_stock_moves.filter(F.col("unit_price") <= 398.5)

move_type_stats = (
    base_moves
    .groupBy("move_type")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

vs_group = (
    base_moves.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

with_trends = (
    vs_group
    .select(
        "*",
        F.lead("median_qty", 1, 0).over(w_sku_id_by_median_qty).alias("next_median_qty"),
        F.lead("unit_price").over(w_all_by_qty_minus_median).alias("next_unit_price"),
        F.count(F.lit(1)).over(w_sku_id_by_median_qty_running).alias("running_count"),
        F.rank().over(w_all_by_qty_minus_median_ties).alias("qty_minus_median_rank"),
    )
)

enriched = (
    with_trends.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.median_qty",
        "s.qty_minus_median",
        "s.running_count",
        "s.qty_minus_median_rank",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

derived = enriched.withColumn("moved_at_plus_7d", F.col("moved_at") + F.expr("INTERVAL 7 DAYS"))

top_rows = (
    derived
    .filter(F.col("qty_minus_median_rank") == 1)
    .drop("qty_minus_median_rank")
)

ranked = (
    top_rows
    .select(
        "id",
        "sku_id",
        "operator",
        "median_qty",
        "qty_minus_median",
        "running_count",
        "warehouse_opened_on",
        "moved_at_plus_7d",
        F.size(F.collect_set("operator").over(w_move_type)).alias("distinct_operator_in_move_type"),
        F.row_number().over(w_operator_sku_id_by_qty_minus).alias("seq_no"),
        F.rank().over(w_operator_sku_id_by_qty_minus_2).alias("qty_minus_median_rank"),
        F.row_number().over(w_all_by_median_qty).alias("median_qty_seq"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_sku_id_by_qty_minus))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "qty_minus_median",
        "warehouse_opened_on",
        "moved_at_plus_7d",
        "distinct_operator_in_move_type",
        "seq_no",
        "median_qty_seq",
        "minutes_since_prev_moved_at",
        F.abs(F.col("qty_minus_median") - -50.0).alias("qty_minus_median_distance"),
        (F.col("moved_at_plus_7d") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_7d_plus_2h"),
    )
    .orderBy(
        F.col("median_qty_seq").desc(),
        F.col("distinct_operator_in_move_type").asc(),
    )
)
