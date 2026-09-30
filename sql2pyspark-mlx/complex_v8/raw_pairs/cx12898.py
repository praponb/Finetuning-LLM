from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_warehouse_ca = Window.partitionBy("warehouse_id").orderBy(F.col("warehouse_capacity").desc(), "id")
w_all_by_warehouse_capacity = Window.orderBy("warehouse_capacity", "id")
w_unit_price_tier_move_type_by = Window.partitionBy("unit_price_tier", "move_type").orderBy(F.col("unit_price").desc(), "id")
w_unit_price_tier_move_type_by_2 = Window.partitionBy("unit_price_tier", "move_type").orderBy(F.col("unit_price").desc())
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_by_warehouse_ca_2 = Window.partitionBy("warehouse_id").orderBy(F.col("warehouse_capacity").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_category_by = Window.partitionBy("warehouse_id", "sku_category").orderBy("qty", "id")
w_unit_price_dense_rank_by_mov = Window.partitionBy("unit_price_dense_rank").orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_moved_at_date_month_by_unit = Window.partitionBy(F.date_trunc("month", "moved_at_date")).orderBy("unit_price_dense_rank", "id")
w_all_by_warehouse_capacity_ch = Window.orderBy("warehouse_capacity_change", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_stock_moves
    .filter(F.col("unit_price") > 130.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.when(F.col("unit_price") >= 330.0, "A").when(F.col("unit_price") >= 112.8, "B").otherwise("C").alias("unit_price_tier"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.lag("sku_weight_kg", 3).over(w_warehouse_id_by_warehouse_ca).alias("prev3_sku_weight_kg"),
        F.rank().over(w_all_by_warehouse_capacity).alias("warehouse_capacity_rank"),
        (F.col("warehouse_capacity") - F.lag("warehouse_capacity").over(w_unit_price_tier_move_type_by)).alias("warehouse_capacity_change"),
        F.dense_rank().over(w_unit_price_tier_move_type_by_2).alias("unit_price_dense_rank"),
        F.round(F.avg("sku_weight_kg").over(w_move_type), 2).alias("move_type_avg_sku_weight_kg"),
        F.lead("unit_price").over(w_all_by_warehouse_capacity).alias("next_unit_price"),
        F.size(F.collect_set("operator").over(w_move_type)).alias("distinct_operator_in_move_type"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.last("moved_at").over(w_warehouse_id_by_warehouse_ca_2).alias("last_moved_at"),
        F.dense_rank().over(w_warehouse_id_sku_category_by).alias("qty_dense_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_unit_price_dense_rank_by_mov).alias("seq_no"),
        F.datediff(F.col("moved_at_date"), F.lag("moved_at_date").over(w_moved_at_date_month_by_unit)).alias("days_since_prev_moved_at_date"),
        F.nth_value("moved_at_date", 2).over(w_all_by_warehouse_capacity_ch).alias("second_moved_at_date"),
    )
)

result = (
    metrics
    .filter(F.col("warehouse_capacity") >= 2614)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price_tier",
        "warehouse_capacity",
        "sku_category",
        "sku_weight_kg",
        "prev3_sku_weight_kg",
        "warehouse_capacity_rank",
        "warehouse_capacity_change",
        "move_type_avg_sku_weight_kg",
        "next_unit_price",
        "distinct_operator_in_move_type",
        "last_moved_at",
        "qty_dense_rank",
        "seq_no",
        "days_since_prev_moved_at_date",
        "second_moved_at_date",
        F.date_add(F.col("second_moved_at_date"), 90).alias("second_moved_at_date_plus_90d"),
    )
    .orderBy("next_unit_price")
)
