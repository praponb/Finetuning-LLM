from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_unit_price_tier = Window.partitionBy("move_type", "unit_price_tier")
w_operator_move_type_by_qty = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").desc(), "id")
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy(F.col("unit_price").asc_nulls_last())
w_move_type_warehouse_id_by_qt = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_operator_by_unit_price_runni = Window.partitionBy("operator").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")
w_operator_move_type_by_qty_ti = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").desc())

recent_moves = (
    df_stock_moves
    .filter((~F.col("move_type").isin("OUTBOUND", "ADJUST")) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.when(F.col("unit_price") >= 372.6, "A").when(F.col("unit_price") >= 215.4, "B").otherwise("C").alias("unit_price_tier"),
    )
)

with_trends = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_or_unknown",
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.last("unit_price").over(w_move_type_by_qty_full).alias("last_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_unit_price_tier), F.lit(0)), 4).alias("unit_price_share_of_move_type_unit_price_tier"),
        F.dense_rank().over(w_operator_move_type_by_qty).alias("qty_dense_rank"),
        F.dense_rank().over(w_operator_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_move_type_unit_price_tier).alias("rows_in_move_type_unit_price_tier"),
        F.sum("qty").over(w_move_type_warehouse_id_by_qt).alias("running_qty"),
        F.sum("qty").over(w_move_type_unit_price_tier).alias("move_type_unit_price_tier_sum_qty"),
        F.round(F.percent_rank().over(w_operator_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.row_number().over(w_operator_by_unit_price).alias("unit_price_seq"),
        F.sum("qty").over(w_operator_by_unit_price_runni).alias("running_qty_2"),
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_by_unit_price)).alias("unit_price_change"),
        F.round(F.percent_rank().over(w_move_type_by_qty), 3).alias("qty_percent_rank"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_unit_price_tier)).alias("distinct_warehouse_id_in_move_type_unit_price_ti"),
        F.round(F.percent_rank().over(w_operator_move_type_by_qty_ti), 3).alias("qty_percent_rank_2"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("qty_dense_rank") <= 5)
    .drop("qty_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "move_type_or_unknown",
        "last_unit_price",
        "unit_price_share_of_move_type_unit_price_tier",
        "unit_price_dense_rank",
        "running_qty",
        "move_type_unit_price_tier_sum_qty",
        "unit_price_percent_rank",
        "unit_price_seq",
        "running_qty_2",
        "unit_price_change",
        "qty_percent_rank",
        "distinct_warehouse_id_in_move_type_unit_price_ti",
        "qty_percent_rank_2",
        F.lower(F.regexp_replace(F.trim(F.col("move_type_or_unknown")), " ", "_")).alias("move_type_or_unknown_slug"),
    )
    .orderBy(
        F.col("running_qty_2").asc_nulls_last(),
        F.col("qty_percent_rank").desc_nulls_first(),
    )
)
