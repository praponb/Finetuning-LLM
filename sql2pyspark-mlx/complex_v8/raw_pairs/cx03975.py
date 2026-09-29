from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at_month").desc(), "id")
w_operator_move_type_by_moved_2 = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_moved_at_month = Window.partitionBy("move_type").orderBy("moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_moved_at_month_2 = Window.partitionBy("move_type").orderBy("moved_at_month", "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_move_type_by_moved_at_month_3 = Window.partitionBy("move_type").orderBy("moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_move_type_by_moved_at_month_4 = Window.partitionBy("move_type").orderBy("moved_at_month")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")

cleaned = (
    df_stock_moves
    .filter(F.col("qty") <= 355)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w_operator_move_type_by_moved).alias("moved_at_month_decile"),
        F.nth_value("unit_price_bucket", 2).over(w_operator_move_type_by_moved_2).alias("second_unit_price_bucket"),
        F.first("unit_price_bucket", ignorenulls=True).over(w_move_type_by_moved_at_month).alias("first_known_unit_price_bucket"),
        F.ntile(3).over(w_move_type_by_moved_at_month_2).alias("moved_at_month_tercile"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_warehouse_id_sku_id), F.lit(0)), 4).alias("unit_price_bucket_share_of_warehouse_id_sku_id"),
        F.last("qty").over(w_move_type_by_moved_at_month_3).alias("last_qty"),
        F.size(F.collect_set("operator").over(w_warehouse_id_move_type)).alias("distinct_operator_in_warehouse_id_move_type"),
        F.rank().over(w_move_type_by_moved_at_month_4).alias("moved_at_month_rank"),
        F.lead("moved_at_month", 3).over(w_all_by_qty).alias("next3_moved_at_month"),
    )
)

grouped = (
    metrics
    .filter(F.col("sku_id") <= 4)
    .groupBy(
        F.year("next3_moved_at_month").alias("next3_moved_at_month_year"),
        F.to_date("moved_at_month").alias("moved_at_month_day"),
    )
    .agg(
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.avg("moved_at_month_decile"), 2).alias("avg_moved_at_month_decile"),
        F.round(F.sum("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket_share_of_warehouse_id_sku_id"), F.lit(0)), 3).alias("unit_price_bucket_to_unit_price_bucket_share_of_"),
        F.round(100 * F.count(F.when(F.col("operator") != "op_ann", 1)) / F.count("*"), 1).alias("pct_operator_op_ann"),
        F.count("*").alias("cnt"),
        F.countDistinct("move_type").alias("distinct_move_types"),
    )
)

result = (
    grouped
    .select(
        "next3_moved_at_month_year",
        "moved_at_month_day",
        "distinct_warehouse_ids",
        "avg_moved_at_month_decile",
        "pct_operator_op_ann",
        "distinct_move_types",
        F.when(F.col("distinct_warehouse_ids") >= 7, "large").when(F.col("distinct_warehouse_ids") >= 3, "medium").otherwise("small").alias("distinct_warehouse_ids_tier"),
    )
    .orderBy("distinct_move_types", "next3_moved_at_month_year")
)
