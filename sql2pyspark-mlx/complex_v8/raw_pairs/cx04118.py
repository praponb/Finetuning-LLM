from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_days_since_move = Window.partitionBy("move_type").orderBy("days_since_moved_at", "id").rowsBetween(-6, Window.currentRow)
w_move_type_by_days_since_move_2 = Window.partitionBy("move_type").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator = Window.partitionBy("operator")
w_sku_is_active = Window.partitionBy("sku_is_active")
w_sku_id_moved_at_year_by_move = Window.partitionBy("sku_id", F.year("moved_at")).orderBy(F.col("moved_at_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_sku_id = Window.partitionBy("sku_id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("move_type").isin("TRANSFER", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_details_sku = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    with_details_sku
    .select(
        "*",
        F.sum("unit_price").over(w_move_type_by_days_since_move).alias("unit_price_sum_7"),
        F.last("moved_at").over(w_move_type_by_days_since_move_2).alias("last_moved_at"),
        F.nth_value("days_since_moved_at", 2).over(w_all_by_moved_at_full).alias("second_days_since_moved_at"),
        F.size(F.collect_set("warehouse_id").over(w_operator)).alias("distinct_warehouse_id_in_operator"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_is_active), F.lit(0)), 4).alias("unit_price_share_of_sku_is_active"),
        F.count(F.lit(1)).over(w_sku_is_active).alias("rows_in_sku_is_active"),
        F.round(F.avg("warehouse_capacity").over(w_sku_id_moved_at_year_by_move), 2).alias("warehouse_capacity_avg_6_prior"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.round(F.avg("warehouse_capacity").over(w_sku_is_active), 2).alias("sku_is_active_avg_warehouse_capacity"),
    )
)

derived = (
    windowed
    .filter(F.col("rows_in_sku_id") > 3)
    .withColumn("moved_at_label", F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00"))
)

result = (
    derived
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "days_since_moved_at",
        "sku_category",
        "sku_labels",
        "warehouse_wh_code",
        "warehouse_capacity",
        "unit_price_sum_7",
        "last_moved_at",
        "second_days_since_moved_at",
        "distinct_warehouse_id_in_operator",
        "rows_in_sku_is_active",
        "warehouse_capacity_avg_6_prior",
        "rows_in_operator",
        "sku_is_active_avg_warehouse_capacity",
        "moved_at_label",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
    .orderBy(F.col("sku_is_active_avg_warehouse_capacity").desc(), "id")
    .limit(20)
)
