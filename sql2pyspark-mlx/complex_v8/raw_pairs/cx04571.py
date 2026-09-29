from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_unit_price_p = Window.partitionBy("warehouse_id").orderBy("unit_price_plus_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price_plus_qty", "id")
w_warehouse_id_by_unit_price_p_2 = Window.partitionBy("warehouse_id").orderBy("unit_price_plus_qty")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_sku_id = Window.partitionBy("sku_id")
w_operator_by_qty = Window.partitionBy("operator").orderBy(F.col("qty").desc_nulls_first(), "id")
w_warehouse_id_by_unit_price_p_3 = Window.partitionBy("warehouse_id").orderBy("unit_price_plus_qty", "id")
w_move_type = Window.partitionBy("move_type")
w_operator_by_qty_vs_sku_id_wa = Window.partitionBy("operator").orderBy(F.col("qty_vs_sku_id_warehouse_id_avg").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_operator_by_qty_vs_sku_id_wa_2 = Window.partitionBy("operator").orderBy(F.col("qty_vs_sku_id_warehouse_id_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_or_unknown_2_move_t = Window.partitionBy("operator_or_unknown_2", "move_type").orderBy(F.col("median_move_type_max_seq_no").desc_nulls_first(), "operator_or_unknown_2", "move_type")
w_operator_or_unknown_2_move_t_2 = Window.partitionBy("operator_or_unknown_2", "move_type").orderBy(F.col("median_move_type_max_seq_no").desc_nulls_first(), "operator_or_unknown_2", "move_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_distinct_sku_ids = Window.partitionBy("row_count", "distinct_sku_ids")

recent = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.count(F.lit(1)).over(w_warehouse_id_by_unit_price_p).alias("running_count"),
        F.lag("unit_price_plus_qty", 1, 0).over(w_warehouse_id_sku_id_by_unit).alias("prev_unit_price_plus_qty"),
        F.rank().over(w_warehouse_id_by_unit_price_p_2).alias("unit_price_plus_qty_rank"),
        F.size(F.collect_set("operator").over(w_sku_id_warehouse_id)).alias("distinct_operator_in_sku_id_warehouse_id"),
        F.round(F.avg("unit_price_plus_qty").over(w_sku_id), 2).alias("sku_id_avg_unit_price_plus_qty"),
        F.round(F.col("unit_price_plus_qty") / F.nullif(F.sum("unit_price_plus_qty").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("unit_price_plus_qty_share_of_sku_id_warehouse_id"),
        F.sum("qty").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_sum_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_warehouse_id), 2).alias("qty_vs_sku_id_warehouse_id_avg"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.row_number().over(w_operator_by_qty).alias("seq_no"),
        F.lead("qty", 2).over(w_warehouse_id_sku_id_by_unit).alias("next2_qty"),
        F.ntile(5).over(w_warehouse_id_sku_id_by_unit).alias("unit_price_plus_qty_quintile"),
        F.lag("unit_price_plus_qty").over(w_warehouse_id_by_unit_price_p_3).alias("prev_unit_price_plus_qty_2"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.when(F.col("seq_no") >= 8, "A").when(F.col("seq_no") >= 4, "B").otherwise("C").alias("seq_no_tier"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown_2"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.max("seq_no").over(w_move_type).alias("move_type_max_seq_no"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.max("rows_in_sku_id").over(w_operator_by_qty_vs_sku_id_wa).alias("rows_in_sku_id_max_3_prior"),
        F.sum("prev_unit_price_plus_qty").over(w_operator_by_qty_vs_sku_id_wa_2).alias("running_prev_unit_price_plus_qty"),
    )
)

stats = (
    ranked
    .groupBy("operator_or_unknown_2", "move_type")
    .agg(
        F.percentile_approx("move_type_max_seq_no", 0.5).alias("median_move_type_max_seq_no"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"), F.col("move_type_max_seq_no")).otherwise(0)).alias("move_type_max_seq_no_move_type_in_adjust_transfe"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
)

ranked_2 = (
    stats
    .select(
        "*",
        F.lag("distinct_sku_ids").over(w_operator_or_unknown_2_move_t).alias("prev_distinct_sku_ids"),
        F.last("median_move_type_max_seq_no").over(w_operator_or_unknown_2_move_t_2).alias("last_median_move_type_max_seq_no"),
        F.round(F.avg("row_count").over(w_row_count_distinct_sku_ids), 2).alias("row_count_distinct_sku_ids_avg_row_count"),
    )
)

result = (
    ranked_2
    .select(
        "operator_or_unknown_2",
        "move_type",
        "row_count",
        "move_type_max_seq_no_move_type_in_adjust_transfe",
        "distinct_sku_ids",
        "row_count_distinct_sku_ids_avg_row_count",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
    .orderBy("row_count")
)
