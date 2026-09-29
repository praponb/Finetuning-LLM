from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_city = Window.partitionBy("operator", "warehouse_city")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_details = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("B%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

enriched = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "warehouse_city",
        "warehouse_capacity",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "moved_at",
        "operator",
        "unit_price",
        F.sum("unit_price").over(w_operator_warehouse_city).alias("operator_warehouse_city_sum_unit_price"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
        F.lag("unit_price").over(w_sku_id_by_moved_at).alias("prev_unit_price"),
        F.max("unit_price").over(w_operator_warehouse_city).alias("operator_warehouse_city_max_unit_price"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
        F.ntile(5).over(w_all_by_moved_at).alias("moved_at_quintile"),
    )
)

totals = (
    windowed
    .groupBy("moved_at_quintile")
    .agg(
        F.round(F.sum("prev_unit_price") / F.nullif(F.sum("moved_at_quintile"), F.lit(0)), 3).alias("prev_unit_price_to_moved_at_quintile_ratio"),
        F.round(F.avg("rn"), 2).alias("avg_rn"),
        F.round(F.avg("moved_at_quintile"), 2).alias("avg_moved_at_quintile"),
    )
)

result = (
    totals
    .select(
        "moved_at_quintile",
        "prev_unit_price_to_moved_at_quintile_ratio",
        "avg_moved_at_quintile",
    )
    .orderBy(
        F.col("avg_moved_at_quintile").desc(),
        F.col("prev_unit_price_to_moved_at_quintile_ratio").desc(),
    )
)
