from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_moved_at_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_move_type = Window.partitionBy("move_type")
w_operator_moved_at_month_by_u = Window.partitionBy("operator", F.month("moved_at")).orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_operator_moved_at_month_by_u_2 = Window.partitionBy("operator", F.month("moved_at")).orderBy(F.col("unit_price").asc_nulls_last())
w_row_count_operator_by_row_co = Window.partitionBy("row_count", "operator").orderBy(F.col("row_count").asc_nulls_last(), "prev_moved_at_ym", "unit_price_dense_rank", "operator")
w_operator = Window.partitionBy("operator")
w_row_count_operator_by_row_co_2 = Window.partitionBy("row_count", "operator").orderBy(F.col("row_count").asc_nulls_last(), "prev_moved_at_ym", "unit_price_dense_rank", "operator").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stock_moves
    .filter((F.col("operator").isin("op_ann", "op_bee")) & (F.col("move_type") == "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

combined_details_sku = (
    valid.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category").isNotNull()), "inner")
    .filter(F.length(F.trim(F.col("sk.sku_code"))) >= 6)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
    )
)

sequenced = (
    combined_details_sku
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "sku_code",
        F.last("unit_price", ignorenulls=True).over(w_operator_by_moved_at_running).alias("last_known_unit_price"),
        F.round(F.percent_rank().over(w_operator_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.round(F.cume_dist().over(w_operator_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.dense_rank().over(w_all_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.round(F.percent_rank().over(w_operator_moved_at_month_by_u), 3).alias("unit_price_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.lag("moved_at").over(w_operator_moved_at_month_by_u).alias("prev_moved_at"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank_2"),
        F.round(F.cume_dist().over(w_operator_moved_at_month_by_u_2), 3).alias("unit_price_cume_dist_2"),
    )
)

per_group = (
    sequenced
    .groupBy(
        F.date_format("prev_moved_at", "yyyy-MM").alias("prev_moved_at_ym"),
        "unit_price_dense_rank",
        "operator",
    )
    .agg(
        F.round(F.sum("moved_at_cume_dist") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("moved_at_cume_dist_to_qty_ratio"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
    )
    .filter(F.col("row_count") >= 2)
)

sequenced_2 = (
    per_group
    .select(
        "prev_moved_at_ym",
        "unit_price_dense_rank",
        "operator",
        "moved_at_cume_dist_to_qty_ratio",
        "row_count",
        "sku_code_list",
        F.lead("unit_price_dense_rank").over(w_row_count_operator_by_row_co).alias("next_unit_price_dense_rank"),
        F.round(F.col("unit_price_dense_rank") - F.avg("unit_price_dense_rank").over(w_operator), 2).alias("unit_price_dense_rank_vs_operator_avg"),
        F.last("unit_price_dense_rank", ignorenulls=True).over(w_row_count_operator_by_row_co_2).alias("last_known_unit_price_dense_rank"),
        F.lag("unit_price_dense_rank").over(w_row_count_operator_by_row_co).alias("prev_unit_price_dense_rank"),
    )
)

result = (
    sequenced_2
    .filter(F.col("prev_moved_at_ym").isNotNull())
    .select(
        "prev_moved_at_ym",
        "operator",
        "moved_at_cume_dist_to_qty_ratio",
        "row_count",
        "sku_code_list",
        "next_unit_price_dense_rank",
        "last_known_unit_price_dense_rank",
    )
    .orderBy("next_unit_price_dense_rank")
)
