from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").asc_nulls_last())
w_sku_id_by_moved_at_prior_6 = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(-6, -1)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_operator_sku_id_by_moved_at_2 = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_sku_id_by_moved_at_3 = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_seq_no_operator = Window.partitionBy("seq_no", "operator")
w_all_by_unit_price_vs_move_ty = Window.orderBy(F.col("unit_price_vs_move_type_operator_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_tercile = Window.partitionBy("qty_tercile")
w_seq_no_by_distinct_move_type = Window.partitionBy("seq_no").orderBy(F.col("distinct_move_types").desc(), "seq_no", "move_type", "qty_tercile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

windowed = (
    filtered_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "operator_slug",
        F.rank().over(w_operator_sku_id_by_moved_at).alias("moved_at_rank"),
        F.max("unit_price").over(w_sku_id_by_moved_at_prior_6).alias("unit_price_max_6_prior"),
        F.ntile(3).over(w_all_by_qty).alias("qty_tercile"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.count(F.lit(1)).over(w_operator_sku_id_by_moved_at_2).alias("running_count"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.sum("qty").over(w_move_type_operator).alias("move_type_operator_sum_qty"),
        F.first("qty").over(w_operator_sku_id_by_moved_at_3).alias("first_qty"),
        F.row_number().over(w_sku_id_by_moved_at).alias("seq_no"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_share_of_move_type_operator"),
        F.min("qty").over(w_move_type_operator).alias("move_type_operator_min_qty"),
    )
)

top_per_group = windowed.filter((F.col("seq_no") <= 5) & (F.col("unit_price") <= 330.0))

ranked = (
    top_per_group
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "operator_slug",
        "qty_tercile",
        "running_count",
        "unit_price_change",
        "first_qty",
        "seq_no",
        "move_type_operator_min_qty",
        F.min("unit_price_max_6_prior").over(w_seq_no_operator).alias("seq_no_operator_min_unit_price_max_6_prior"),
        F.round(F.col("moved_at_rank") - F.avg("moved_at_rank").over(w_seq_no_operator), 2).alias("moved_at_rank_vs_seq_no_operator_avg"),
        F.size(F.collect_set("sku_id").over(w_seq_no_operator)).alias("distinct_sku_id_in_seq_no_operator"),
        F.nth_value("running_count", 2).over(w_all_by_unit_price_vs_move_ty).alias("second_running_count"),
    )
)

summary = (
    ranked
    .groupBy("seq_no", "move_type", "qty_tercile")
    .agg(
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

sequenced = (
    summary
    .select(
        "seq_no",
        "move_type",
        "qty_tercile",
        "distinct_move_types",
        "n_rows",
        F.min("distinct_move_types").over(w_qty_tercile).alias("qty_tercile_min_distinct_move_types"),
        F.size(F.collect_set("move_type").over(w_qty_tercile)).alias("distinct_move_type_in_qty_tercile"),
        F.nth_value("seq_no", 2).over(w_seq_no_by_distinct_move_type).alias("second_seq_no"),
        F.round(F.col("qty_tercile") - F.avg("qty_tercile").over(w_qty_tercile), 2).alias("qty_tercile_vs_qty_tercile_avg"),
    )
)

result = (
    sequenced
    .select(
        "seq_no",
        "distinct_move_types",
        "n_rows",
        "qty_tercile_min_distinct_move_types",
        "distinct_move_type_in_qty_tercile",
        "qty_tercile_vs_qty_tercile_avg",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.round(F.col("second_seq_no") / F.nullif(F.col("distinct_move_types"), F.lit(0)), 3).alias("second_seq_no_per_distinct_move_types"),
    )
    .orderBy(F.col("n_rows").desc())
)
