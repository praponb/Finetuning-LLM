from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy("qty")
w_sku_id = Window.partitionBy("sku_id")
w_sku_category_sku_id_by_unit = Window.partitionBy("sku_category", "sku_id").orderBy(F.col("unit_price").desc(), "id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_all_by_qty = Window.orderBy("qty", "id")
w_sku_category = Window.partitionBy("sku_category")
w_all_by_unit_price = Window.orderBy("unit_price", "id")

valid_moves = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
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

with_details_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.category").alias("sku_category"),
    )
)

with_trends = (
    with_details_sku
    .select(
        "id",
        "move_type",
        "qty",
        "unit_price",
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.sum("qty").over(w_sku_id).alias("sku_id_sum_qty"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.cume_dist().over(w_sku_category_sku_id_by_unit), 3).alias("unit_price_cume_dist"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_unit_price"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.round(F.avg("qty").over(w_sku_category), 2).alias("sku_category_avg_qty"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
        F.sum("qty").over(w_sku_category).alias("sku_category_sum_qty"),
    )
)

derived = (
    with_trends
    .filter(F.col("last_known_unit_price") > 160.8)
    .select(
        "*",
        F.concat(F.col("move_type"), F.lit("#"), F.col("unit_price_quintile").cast("string")).alias("move_type_ref"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

result = (
    derived
    .filter(F.col("move_type_ref").isNotNull())
    .select(
        "id",
        "move_type",
        "qty",
        "unit_price",
        "qty_cume_dist",
        "sku_id_sum_qty",
        "distinct_warehouse_id_in_sku_id",
        "unit_price_cume_dist",
        "second_qty",
        "last_known_unit_price",
        "moved_at_cume_dist",
        "unit_price_change",
        "sku_category_sum_qty",
        "qty_per_unit_price",
        F.lower(F.trim(F.col("move_type_ref"))).alias("move_type_ref_clean"),
    )
    .orderBy(F.col("sku_category_sum_qty").desc(), "id")
    .limit(20)
)
