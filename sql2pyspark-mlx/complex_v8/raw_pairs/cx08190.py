from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_moved_at_p = Window.orderBy(F.col("days_since_moved_at").desc(), "id").rowsBetween(-6, -1)
w_sku_category_move_type = Window.partitionBy("sku_category", "move_type")
w_all_by_days_since_moved_at_t = Window.orderBy(F.col("days_since_moved_at").desc())
w_sku_category_move_type_by_da = Window.partitionBy("sku_category", "move_type").orderBy("days_since_moved_at", "id")
w_moved_at_year_sku_category_b = Window.partitionBy(F.year("moved_at"), "sku_category").orderBy("sku_unit_cost", "id")
w_all_by_days_since_moved_at_r = Window.orderBy(F.col("days_since_moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at_running = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_moved_at_year_sku_category_b_2 = Window.partitionBy(F.year("moved_at"), "sku_category").orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_sku_category_move_type_by_da_2 = Window.partitionBy("sku_category", "move_type").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_year = Window.partitionBy(F.year("moved_at"))
w_all_by_moved_at_dense_rank = Window.orderBy("moved_at_dense_rank", "id")
w_all_by_running_sku_unit_cost = Window.orderBy(F.col("running_sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_seq_no_prior = Window.partitionBy("warehouse_id").orderBy("seq_no", "id").rowsBetween(-1, -1)
w_warehouse_id_by_seq_no_runni = Window.partitionBy("warehouse_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_sku_unit_cost_2 = Window.orderBy(F.col("running_sku_unit_cost").desc(), "id")

scoped = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") == "Toys")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.days_since_moved_at",
        "s.operator_ref",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.round(F.avg("sku_unit_cost").over(w_all_by_days_since_moved_at_p), 2).alias("sku_unit_cost_avg_6_prior"),
        F.round(F.avg("sku_unit_cost").over(w_sku_category_move_type), 2).alias("sku_category_move_type_avg_sku_unit_cost"),
        F.round(F.cume_dist().over(w_all_by_days_since_moved_at_t), 3).alias("days_since_moved_at_cume_dist"),
        F.lag("sku_unit_cost", 1, 0).over(w_sku_category_move_type_by_da).alias("prev_sku_unit_cost"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_sku_category_move_type), F.lit(0)), 4).alias("sku_unit_cost_share_of_sku_category_move_type"),
        F.round(F.percent_rank().over(w_moved_at_year_sku_category_b), 3).alias("sku_unit_cost_percent_rank"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_days_since_moved_at_r).alias("last_known_moved_at"),
        F.max("days_since_moved_at").over(w_sku_id_by_moved_at_running).alias("max_days_since_moved_at_so_far"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_sku_category_move_type), F.lit(0)), 4).alias("days_since_moved_at_share_of_sku_category_move_t"),
        F.round(F.cume_dist().over(w_sku_category_move_type_by_da), 3).alias("days_since_moved_at_cume_dist_2"),
        (F.col("sku_unit_cost") - F.lag("sku_unit_cost").over(w_sku_id_by_moved_at)).alias("sku_unit_cost_change"),
        F.dense_rank().over(w_sku_id_by_moved_at).alias("moved_at_dense_rank"),
        F.nth_value("sku_unit_cost", 2).over(w_moved_at_year_sku_category_b_2).alias("second_sku_unit_cost"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.sum("sku_unit_cost").over(w_sku_category_move_type_by_da_2).alias("running_sku_unit_cost"),
        F.row_number().over(w_sku_id_by_moved_at).alias("seq_no"),
    )
)

top_rows = sequenced.filter((F.col("seq_no") <= 2) & (F.col("warehouse_id") >= 4))

ranked = (
    top_rows
    .select(
        "*",
        F.round(F.col("sku_unit_cost_percent_rank") - F.avg("sku_unit_cost_percent_rank").over(w_moved_at_year), 2).alias("sku_unit_cost_percent_rank_vs_moved_at_year_avg"),
        (F.col("days_since_moved_at") - F.lag("days_since_moved_at").over(w_all_by_moved_at_dense_rank)).alias("days_since_moved_at_change"),
        F.size(F.collect_set("move_type").over(w_moved_at_year)).alias("distinct_move_type_in_moved_at_year"),
        F.last("sku_unit_cost", ignorenulls=True).over(w_all_by_running_sku_unit_cost).alias("last_known_sku_unit_cost"),
        F.round(F.avg("seq_no").over(w_warehouse_id_by_seq_no_prior), 2).alias("seq_no_avg_1_prior"),
        F.max("second_sku_unit_cost").over(w_warehouse_id_by_seq_no_runni).alias("max_second_sku_unit_cost_so_far"),
        F.ntile(10).over(w_all_by_running_sku_unit_cost_2).alias("running_sku_unit_cost_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "days_since_moved_at",
        "operator_ref",
        "sku_unit_cost",
        "sku_category",
        "sku_unit_cost_share_of_sku_category_move_type",
        "sku_unit_cost_percent_rank",
        "last_known_moved_at",
        "days_since_moved_at_share_of_sku_category_move_t",
        "days_since_moved_at_cume_dist_2",
        "sku_unit_cost_change",
        "moved_at_dense_rank",
        "second_sku_unit_cost",
        "rows_in_sku_id",
        "running_sku_unit_cost",
        "seq_no",
        "distinct_move_type_in_moved_at_year",
        "last_known_sku_unit_cost",
        "seq_no_avg_1_prior",
        "max_second_sku_unit_cost_so_far",
        F.greatest(F.col("prev_sku_unit_cost"), F.col("sku_unit_cost")).alias("greatest_prev_sku_unit_cost_sku_unit_cost"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), "id")
    .limit(5)
)
