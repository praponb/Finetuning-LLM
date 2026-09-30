from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month = Window.partitionBy("moved_at_month")
w_sku_id_moved_at_month_by_uni = Window.partitionBy("sku_id", "moved_at_month").orderBy("unit_price")
w_sku_id_moved_at_month_by_uni_2 = Window.partitionBy("sku_id", "moved_at_month").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
)

combined_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.moved_at_month",
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

sequenced = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "sku_category",
        "sku_code",
        F.count(F.lit(1)).over(w_moved_at_month).alias("rows_in_moved_at_month"),
        F.round(F.col("qty") - F.avg("qty").over(w_moved_at_month), 2).alias("qty_vs_moved_at_month_avg"),
        F.size(F.collect_set("move_type").over(w_moved_at_month)).alias("distinct_move_type_in_moved_at_month"),
        F.round(F.avg("moved_at_month").over(w_moved_at_month), 2).alias("moved_at_month_avg_moved_at_month"),
        F.round(F.cume_dist().over(w_sku_id_moved_at_month_by_uni), 3).alias("unit_price_cume_dist"),
        F.sum("qty").over(w_moved_at_month).alias("moved_at_month_sum_qty"),
        F.round(F.avg("unit_price").over(w_moved_at_month), 2).alias("moved_at_month_avg_unit_price"),
        F.nth_value("unit_price", 2).over(w_sku_id_moved_at_month_by_uni_2).alias("second_unit_price"),
    )
)

result = (
    sequenced
    .filter(F.col("unit_price_cume_dist") >= 0.2)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "sku_category",
        "rows_in_moved_at_month",
        "qty_vs_moved_at_month_avg",
        "distinct_move_type_in_moved_at_month",
        "moved_at_month_avg_moved_at_month",
        "unit_price_cume_dist",
        "second_unit_price",
        F.abs(F.col("moved_at_month_sum_qty") - 2500).alias("moved_at_month_sum_qty_distance"),
        F.round(F.col("unit_price_cume_dist") * 1.07, 2).alias("unit_price_cume_dist_adj"),
    )
    .orderBy(F.col("sku_id").desc(), "rows_in_moved_at_month")
)
