from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_len_by_moved_at = Window.partitionBy("move_type_len").orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("moved_at").desc())
w_move_type_len_by_moved_at_ti = Window.partitionBy("move_type_len").orderBy("moved_at")
w_move_type_len_by_warehouse_i = Window.partitionBy("move_type_len").orderBy(F.col("warehouse_id_max_sku_unit_cost").asc_nulls_last(), "id")
w_move_type_len_by_warehouse_i_2 = Window.partitionBy("move_type_len").orderBy(F.col("warehouse_id_max_sku_unit_cost").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_len_warehouse_id_b = Window.partitionBy("move_type_len", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_distinct_ids = Window.partitionBy("distinct_ids")
w_n_rows_by_total_min_move_typ = Window.partitionBy("n_rows").orderBy("total_min_move_type_len_so_far")

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("OUTBOUND", "TRANSFER", "ADJUST")) & (F.hour(F.col("moved_at")).between(7, 13)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

joined_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_type_len",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    joined_warehouse
    .select(
        "*",
        F.round(F.col("move_type_len") / F.nullif(F.sum("move_type_len").over(w_warehouse_id), F.lit(0)), 4).alias("move_type_len_share_of_warehouse_id"),
        F.lag("moved_at").over(w_move_type_len_by_moved_at).alias("prev_moved_at"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.nth_value("warehouse_opened_on", 2).over(w_warehouse_opened_on_month_by).alias("second_warehouse_opened_on"),
        F.max("sku_unit_cost").over(w_warehouse_id).alias("warehouse_id_max_sku_unit_cost"),
        F.lead("moved_at", 2).over(w_all_by_moved_at).alias("next2_moved_at"),
        F.dense_rank().over(w_warehouse_opened_on_month_by_2).alias("moved_at_dense_rank_2"),
        F.dense_rank().over(w_move_type_len_by_moved_at_ti).alias("moved_at_dense_rank_3"),
    )
)

leaders = (
    metrics
    .filter((F.col("moved_at_dense_rank_3") == 1) & (F.year(F.col("warehouse_opened_on")) == 2024))
    .drop("moved_at_dense_rank_3")
)

with_trends = (
    leaders
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "sku_unit_cost",
        "prev_moved_at",
        "moved_at_dense_rank",
        "second_warehouse_opened_on",
        "warehouse_id_max_sku_unit_cost",
        F.ntile(4).over(w_move_type_len_by_warehouse_i).alias("warehouse_id_max_sku_unit_cost_quartile"),
        F.min("move_type_len").over(w_move_type_len_by_warehouse_i_2).alias("min_move_type_len_so_far"),
        F.round(F.avg("moved_at_dense_rank").over(w_move_type_len_warehouse_id_b), 2).alias("moved_at_dense_rank_avg_7"),
    )
)

stats = (
    with_trends
    .filter(F.col("warehouse_id_max_sku_unit_cost_quartile") < 7)
    .groupBy(
        F.date_trunc("month", "second_warehouse_opened_on").alias("second_warehouse_opened_on_month"),
        "sku_id",
        "warehouse_id",
    )
    .agg(
        F.min("sku_unit_cost").alias("min_sku_unit_cost"),
        F.count("*").alias("n_rows"),
        F.sum("min_move_type_len_so_far").alias("total_min_move_type_len_so_far"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("min_move_type_len_so_far").alias("max_min_move_type_len_so_far"),
    )
)

ranked = (
    stats
    .withColumn("distinct_ids_share_of_distinct_ids", F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_distinct_ids), F.lit(0)), 4))
    .withColumn("total_min_move_type_len_so_far_cume_dist", F.round(F.cume_dist().over(w_n_rows_by_total_min_move_typ), 3))
)

result = (
    ranked
    .select(
        "second_warehouse_opened_on_month",
        "sku_id",
        "warehouse_id",
        "min_sku_unit_cost",
        "max_min_move_type_len_so_far",
        "distinct_ids_share_of_distinct_ids",
        "total_min_move_type_len_so_far_cume_dist",
    )
    .orderBy("warehouse_id")
)
