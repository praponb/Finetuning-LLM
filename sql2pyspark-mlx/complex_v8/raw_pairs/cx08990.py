from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_group_move_type_by = Window.partitionBy("operator_group", "move_type").orderBy(F.col("unit_price_plus_qty").asc_nulls_last(), "id")
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_operator_group = Window.partitionBy("operator_group")
w_operator_group_move_type_by_2 = Window.partitionBy("operator_group", "move_type").orderBy(F.col("unit_price_plus_qty").asc_nulls_last())
w_operator_group_move_type_by_3 = Window.partitionBy("operator_group", "move_type").orderBy(F.col("unit_price_plus_qty").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_operator_group_warehouse_id = Window.partitionBy("operator_group", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_operator_group_warehouse_id_2 = Window.partitionBy("operator_group", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_group_move_type_by_4 = Window.partitionBy("operator_group", "move_type").orderBy(F.col("unit_price_plus_qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .filter(F.col("qty") >= 189)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_ann", "op_bee"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

metrics = (
    filtered_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "operator_group",
        "unit_price_plus_qty",
        F.row_number().over(w_operator_group_move_type_by).alias("unit_price_plus_qty_seq"),
        F.row_number().over(w_sku_id_operator_by_moved_at).alias("seq_no"),
        F.size(F.collect_set("sku_id").over(w_operator_group)).alias("distinct_sku_id_in_operator_group"),
        F.round(F.cume_dist().over(w_operator_group_move_type_by), 3).alias("unit_price_plus_qty_cume_dist"),
        F.rank().over(w_operator_group_move_type_by_2).alias("unit_price_plus_qty_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_group), F.lit(0)), 4).alias("unit_price_share_of_operator_group"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_group), F.lit(0)), 4).alias("qty_share_of_operator_group"),
        F.round(F.percent_rank().over(w_operator_group_move_type_by_2), 3).alias("unit_price_plus_qty_percent_rank"),
        F.rank().over(w_sku_id_operator_by_moved_at).alias("moved_at_rank"),
        F.lag("moved_at").over(w_sku_id_operator_by_moved_at).alias("prev_moved_at"),
        F.count(F.lit(1)).over(w_operator_group).alias("rows_in_operator_group"),
        F.round(F.avg("unit_price_plus_qty").over(w_operator_group_move_type_by_3), 2).alias("unit_price_plus_qty_avg_2"),
        F.lag("unit_price").over(w_operator_group_warehouse_id).alias("prev_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w_operator_group_warehouse_id_2).alias("first_known_unit_price"),
        F.last("unit_price_plus_qty", ignorenulls=True).over(w_operator_group_move_type_by_4).alias("last_known_unit_price_plus_qty"),
    )
)

grouped = (
    metrics
    .filter(F.col("rows_in_operator_group") <= 17)
    .groupBy("distinct_sku_id_in_operator_group")
    .agg(
        F.round(F.stddev("unit_price_plus_qty_seq"), 2).alias("stddev_unit_price_plus_qty_seq"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum("unit_price_plus_qty_seq").alias("total_unit_price_plus_qty_seq"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("operator") == "op_chan", F.col("last_known_unit_price_plus_qty")).otherwise(0)).alias("last_known_unit_price_plus_qty_operator_op_chan"),
    )
)

result = (
    grouped
    .filter(F.hour(F.col("last_moved_at")).between(7, 13))
    .select(
        "distinct_sku_id_in_operator_group",
        "stddev_unit_price_plus_qty_seq",
        "last_moved_at",
        "total_unit_price_plus_qty_seq",
        "row_count",
        F.to_date(F.col("last_moved_at")).alias("last_moved_at_date"),
    )
)
