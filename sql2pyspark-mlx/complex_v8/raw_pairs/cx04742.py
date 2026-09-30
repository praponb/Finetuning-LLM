from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_sku_category_by_sku_unit_cos = Window.partitionBy("sku_category").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_moved_at_year = Window.partitionBy("sku_id", F.year("moved_at"))
w_sku_category_by_sku_unit_cos_2 = Window.partitionBy("sku_category").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-3, -1)
w_sku_category_warehouse_id_by = Window.partitionBy("sku_category", "warehouse_id").orderBy("sku_unit_cost", "id")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_sku_category_by_sku_unit_cos_3 = Window.partitionBy("sku_category").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_second_unit = Window.partitionBy("warehouse_id").orderBy("second_unit_price", "id")
w_sku_category_distinct_wareho = Window.partitionBy("sku_category", "distinct_warehouse_id_in_sku_id_moved_at_year").orderBy("qty_distance_sum_3_prior", "id")
w_all_by_unit_price_vs_sku_id = Window.orderBy("unit_price_vs_sku_id_avg")
w_move_type = Window.partitionBy("move_type")

base_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 148).alias("qty_distance"),
    )
)

combined_details = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.qty_distance",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "qty_distance",
        "sku_category",
        "sku_weight_kg",
        "sku_unit_cost",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.nth_value("unit_price", 2).over(w_sku_category_by_sku_unit_cos).alias("second_unit_price"),
        F.round(F.avg("qty_distance").over(w_sku_id_moved_at_year), 2).alias("sku_id_moved_at_year_avg_qty_distance"),
        F.sum("qty_distance").over(w_sku_category_by_sku_unit_cos_2).alias("qty_distance_sum_3_prior"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.ntile(4).over(w_sku_category_warehouse_id_by).alias("sku_unit_cost_quartile"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_moved_at)).alias("unit_price_change"),
        F.count(F.lit(1)).over(w_sku_id_moved_at_year).alias("rows_in_sku_id_moved_at_year"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_moved_at_year)).alias("distinct_warehouse_id_in_sku_id_moved_at_year"),
        F.min("unit_price").over(w_sku_id_moved_at_year).alias("sku_id_moved_at_year_min_unit_price"),
        F.min("sku_unit_cost").over(w_sku_category_by_sku_unit_cos_3).alias("min_sku_unit_cost_so_far"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.row_number().over(w_warehouse_id_by_second_unit).alias("second_unit_price_seq"),
        F.ntile(3).over(w_sku_category_distinct_wareho).alias("qty_distance_sum_3_prior_tercile"),
        F.dense_rank().over(w_all_by_unit_price_vs_sku_id).alias("unit_price_vs_sku_id_avg_dense_rank"),
        F.rank().over(w_all_by_unit_price_vs_sku_id).alias("unit_price_vs_sku_id_avg_rank"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "qty_distance",
        "sku_category",
        "sku_unit_cost",
        "unit_price_vs_sku_id_avg",
        "second_unit_price",
        "sku_id_moved_at_year_avg_qty_distance",
        "qty_distance_sum_3_prior",
        "sku_unit_cost_quartile",
        "unit_price_change",
        "rows_in_sku_id_moved_at_year",
        "moved_at_label",
        "second_unit_price_seq",
        "qty_distance_sum_3_prior_tercile",
        "unit_price_vs_sku_id_avg_dense_rank",
        "unit_price_vs_sku_id_avg_rank",
        "rows_in_move_type",
    )
    .orderBy(
        F.col("sku_unit_cost_quartile").asc_nulls_last(),
        F.col("qty_distance_sum_3_prior_tercile").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
