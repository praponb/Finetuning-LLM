from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator_operator_len_by_ope = Window.partitionBy("operator", "operator_len").orderBy(F.col("operator_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_len_by_moved_at_ful = Window.partitionBy("operator_len").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_ids_full = Window.orderBy("distinct_ids", "operator", "moved_at_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows = Window.partitionBy("n_rows")
w_all_by_distinct_ids = Window.orderBy("distinct_ids", "operator", "moved_at_ym")
w_distinct_ids_n_rows_by_disti = Window.partitionBy("distinct_ids", "n_rows").orderBy("distinct_ids", "operator", "moved_at_ym")
w_all_by_distinct_ids_vs_n_row = Window.orderBy("distinct_ids_vs_n_rows_avg", "operator", "moved_at_ym")
w_operator_by_seq_no_ties = Window.partitionBy("operator").orderBy(F.col("seq_no").asc_nulls_last())

scoped_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(2, 3)) & (~F.col("move_type").isin("OUTBOUND", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

sequenced = (
    scoped_moves
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_share_of_sku_id_operator"),
        F.last("qty_plus_unit_price").over(w_operator_operator_len_by_ope).alias("last_qty_plus_unit_price"),
        F.nth_value("qty", 2).over(w_operator_len_by_moved_at_ful).alias("second_qty"),
    )
)

summary = (
    sequenced
    .groupBy("operator", F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"))
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("unit_price_adj") / F.nullif(F.sum("unit_price_share_of_sku_id_operator"), F.lit(0)), 3).alias("unit_price_adj_to_unit_price_share_of_sku_id_ope"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

windowed = (
    summary
    .select(
        "operator",
        "moved_at_ym",
        "n_rows",
        "distinct_ids",
        F.nth_value("distinct_ids", 2).over(w_all_by_distinct_ids_full).alias("second_distinct_ids"),
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_n_rows), 2).alias("distinct_ids_vs_n_rows_avg"),
        F.count(F.lit(1)).over(w_n_rows).alias("rows_in_n_rows"),
        F.row_number().over(w_all_by_distinct_ids).alias("seq_no"),
        F.lag("n_rows", 1, 0).over(w_distinct_ids_n_rows_by_disti).alias("prev_n_rows"),
        F.min("n_rows").over(w_n_rows).alias("n_rows_min_n_rows"),
    )
)

leaders = windowed.filter((F.col("seq_no") <= 2) & (F.col("rows_in_n_rows") >= 7))

annotated = (
    leaders
    .select(
        "operator",
        "moved_at_ym",
        "second_distinct_ids",
        "distinct_ids_vs_n_rows_avg",
        "seq_no",
        "prev_n_rows",
        F.least(F.col("n_rows"), F.col("second_distinct_ids")).alias("least_n_rows_second_distinct_ids"),
        F.upper(F.trim(F.col("moved_at_ym"))).alias("moved_at_ym_clean"),
        F.when(F.col("operator").isin("op_dew", "op_bee"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)

with_windows = (
    annotated
    .withColumn("next3_seq_no", F.lead("seq_no", 3, 0).over(w_all_by_distinct_ids_vs_n_row))
    .withColumn("seq_no_rank", F.rank().over(w_operator_by_seq_no_ties))
)

result = (
    with_windows
    .filter(F.col("distinct_ids_vs_n_rows_avg") >= -7.0)
    .select(
        "operator",
        "moved_at_ym",
        "distinct_ids_vs_n_rows_avg",
        "seq_no",
        "prev_n_rows",
        "least_n_rows_second_distinct_ids",
        "moved_at_ym_clean",
        "next3_seq_no",
        "seq_no_rank",
        F.least(F.col("seq_no"), F.col("next3_seq_no")).alias("least_seq_no_next3_seq_no"),
        F.abs(F.col("seq_no_rank") - 7).alias("seq_no_rank_distance"),
    )
    .orderBy(F.col("distinct_ids_vs_n_rows_avg").desc())
)
