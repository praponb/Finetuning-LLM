from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_hour_month = Window.partitionBy(F.month("moved_at_hour"))
w_sku_id_by_sku_weight_kg = Window.partitionBy("sku_id").orderBy(F.col("sku_weight_kg").desc(), "id")
w_sku_category_move_type_by_mo = Window.partitionBy("sku_category", "move_type").orderBy("moved_at_hour")
w_all_by_moved_at_hour_full = Window.orderBy(F.col("moved_at_hour").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_hour = Window.orderBy(F.col("moved_at_hour").asc_nulls_last(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_unit_price_m = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_sku_category_move_type_by_mo_2 = Window.partitionBy("sku_category", "move_type").orderBy("moved_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_stock_moves
    .filter((F.col("unit_price") < 387.1) & (F.col("sku_id") <= 4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

joined_sku = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_hour",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_windows = (
    joined_sku
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_moved_at_hour_month), F.lit(0)), 4).alias("unit_price_share_of_moved_at_hour_month"),
        F.lead("unit_price", 2).over(w_sku_id_by_sku_weight_kg).alias("next2_unit_price"),
        F.rank().over(w_sku_category_move_type_by_mo).alias("moved_at_hour_rank"),
        F.round(F.avg("sku_weight_kg").over(w_moved_at_hour_month), 2).alias("moved_at_hour_month_avg_sku_weight_kg"),
        F.nth_value("moved_at_hour", 2).over(w_all_by_moved_at_hour_full).alias("second_moved_at_hour"),
        F.round(F.percent_rank().over(w_all_by_moved_at_hour), 3).alias("moved_at_hour_percent_rank"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.sum("qty").over(w_warehouse_id_by_unit_price_m).alias("qty_sum_7"),
        F.round(F.cume_dist().over(w_all_by_moved_at_hour), 3).alias("moved_at_hour_cume_dist"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_category_move_type_by_mo_2).alias("second_sku_weight_kg"),
        F.size(F.collect_set("sku_id").over(w_moved_at_hour_month)).alias("distinct_sku_id_in_moved_at_hour_month"),
    )
)

scored = with_windows.withColumn("second_moved_at_hour_year", F.year(F.col("second_moved_at_hour")))

result = (
    scored
    .filter(F.col("moved_at_hour_rank") >= 4)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "moved_at_hour",
        "sku_category",
        "sku_weight_kg",
        "unit_price_share_of_moved_at_hour_month",
        "moved_at_hour_rank",
        "moved_at_hour_month_avg_sku_weight_kg",
        "second_moved_at_hour",
        "moved_at_hour_percent_rank",
        "rows_in_warehouse_id",
        "qty_sum_7",
        "moved_at_hour_cume_dist",
        "distinct_sku_id_in_moved_at_hour_month",
        "second_moved_at_hour_year",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)
