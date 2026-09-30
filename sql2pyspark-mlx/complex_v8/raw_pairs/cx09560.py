from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_by_moved_at_ful = Window.partitionBy("sku_category").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category_warehouse_id_by = Window.partitionBy("sku_category", "warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_category_by_moved_at_tie = Window.partitionBy("sku_category").orderBy("moved_at")
w_warehouse_id_sku_id_by_moved_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc())
w_sku_category_warehouse_id_by_2 = Window.partitionBy("sku_category", "warehouse_id").orderBy(F.col("moved_at").desc())
w_sku_category_by_moved_at = Window.partitionBy("sku_category").orderBy("moved_at", "id")
w_sku_category_warehouse_id_by_3 = Window.partitionBy("sku_category", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at", "id")
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_category_by_moved_at_run = Window.partitionBy("sku_category").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_moved_at_dense_rank_2_by = Window.partitionBy("rn", "moved_at_dense_rank_2").orderBy("second_warehouse_opened_on")
w_last_known_moved_at_year_by = Window.partitionBy(F.year("last_known_moved_at")).orderBy(F.col("prev_moved_at_2").asc_nulls_last(), "id")
w_second_moved_at_year = Window.partitionBy(F.year("second_moved_at"))
w_rn_moved_at_dense_rank_2_by_2 = Window.partitionBy("rn", "moved_at_dense_rank_2").orderBy("second_warehouse_opened_on", "id")
w_last_known_moved_at_year_by_2 = Window.partitionBy(F.year("last_known_moved_at")).orderBy(F.col("prev_moved_at_2").asc_nulls_last())

prepared_moves = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_ann", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

combined_details_warehouse = (
    prepared_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

ranked = (
    combined_details_warehouse
    .select(
        "id",
        "move_type",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_wh_code",
        "sku_category",
        "sku_labels",
        F.last("moved_at").over(w_sku_category_by_moved_at_ful).alias("last_moved_at"),
        F.lag("moved_at").over(w_warehouse_id_sku_id_by_moved).alias("prev_moved_at"),
        F.nth_value("moved_at", 2).over(w_sku_category_by_moved_at_ful).alias("second_moved_at"),
        F.nth_value("warehouse_opened_on", 2).over(w_warehouse_id_sku_id_by_moved_2).alias("second_warehouse_opened_on"),
        F.row_number().over(w_sku_category_warehouse_id_by).alias("rn"),
        F.rank().over(w_sku_category_by_moved_at_tie).alias("moved_at_rank"),
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_moved_3), 3).alias("moved_at_cume_dist"),
        F.dense_rank().over(w_sku_category_warehouse_id_by_2).alias("moved_at_dense_rank"),
        F.lag("moved_at").over(w_sku_category_by_moved_at).alias("prev_moved_at_2"),
        F.first("warehouse_opened_on", ignorenulls=True).over(w_sku_category_warehouse_id_by_3).alias("first_known_warehouse_opened_on"),
        F.dense_rank().over(w_sku_id_warehouse_id_by_moved).alias("moved_at_dense_rank_2"),
        F.round(F.cume_dist().over(w_sku_category_warehouse_id_by_2), 3).alias("moved_at_cume_dist_2"),
        F.last("moved_at", ignorenulls=True).over(w_sku_id_warehouse_id_by_moved_2).alias("last_known_moved_at"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_sku_id_warehouse_id_by_moved)).alias("days_since_prev_warehouse_opened_on"),
        F.first("moved_at", ignorenulls=True).over(w_sku_category_by_moved_at_run).alias("first_known_moved_at"),
    )
)

top_per_group = ranked.filter(F.col("moved_at_rank") <= 2)

with_trends = (
    top_per_group
    .select(
        "*",
        F.round(F.percent_rank().over(w_rn_moved_at_dense_rank_2_by), 3).alias("second_warehouse_opened_on_percent_rank"),
        F.lag("prev_moved_at").over(w_last_known_moved_at_year_by).alias("prev_prev_moved_at"),
        F.size(F.collect_set("sku_category").over(w_second_moved_at_year)).alias("distinct_sku_category_in_second_moved_at_year"),
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_second_moved_at_year), F.lit(0)), 4).alias("rn_share_of_second_moved_at_year"),
        F.row_number().over(w_rn_moved_at_dense_rank_2_by_2).alias("second_warehouse_opened_on_seq"),
        F.round(F.cume_dist().over(w_rn_moved_at_dense_rank_2_by), 3).alias("second_warehouse_opened_on_cume_dist"),
        F.round(F.col("moved_at_cume_dist") - F.avg("moved_at_cume_dist").over(w_second_moved_at_year), 2).alias("moved_at_cume_dist_vs_second_moved_at_year_avg"),
        F.round(F.percent_rank().over(w_last_known_moved_at_year_by_2), 3).alias("prev_moved_at_2_percent_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("prev_moved_at_2_percent_rank") <= 0.7)
    .select(
        "id",
        "move_type",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_wh_code",
        "sku_category",
        "last_moved_at",
        "prev_moved_at",
        "second_warehouse_opened_on",
        "rn",
        "moved_at_cume_dist",
        "moved_at_dense_rank",
        "first_known_warehouse_opened_on",
        "moved_at_dense_rank_2",
        "moved_at_cume_dist_2",
        "last_known_moved_at",
        "days_since_prev_warehouse_opened_on",
        "first_known_moved_at",
        "second_warehouse_opened_on_percent_rank",
        "prev_prev_moved_at",
        "rn_share_of_second_moved_at_year",
        "second_warehouse_opened_on_seq",
        "second_warehouse_opened_on_cume_dist",
        "prev_moved_at_2_percent_rank",
    )
    .orderBy(F.col("moved_at_dense_rank_2").desc())
)
