from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_all_by_first_moved_at_full = Window.orderBy("first_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_warehouse_capacity_fu = Window.orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_moved_at_moving = Window.orderBy("first_moved_at", "id").rowsBetween(-2, Window.currentRow)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")

cleaned = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(8, 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_minus_unit_price",
        "s.greatest_qty_unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

summarized_rows = (
    joined.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.first_moved_at")
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.last("unit_price").over(w_all_by_first_moved_at_full).alias("last_unit_price"),
        F.nth_value("first_moved_at", 2).over(w_all_by_warehouse_capacity_fu).alias("second_first_moved_at"),
        F.round(F.avg("warehouse_capacity").over(w_all_by_first_moved_at_moving), 2).alias("warehouse_capacity_avg_3"),
        F.last("first_moved_at").over(w_all_by_warehouse_capacity_fu).alias("last_first_moved_at"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_operator)).alias("distinct_move_type_in_warehouse_id_operator"),
    )
)

enriched_sku = (
    metrics.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.qty_minus_unit_price",
        "s.greatest_qty_unit_price",
        "s.warehouse_capacity",
        "s.warehouse_city",
        "s.first_moved_at",
        "s.last_unit_price",
        "s.second_first_moved_at",
        "s.last_first_moved_at",
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
    )
)

result = (
    enriched_sku
    .select(
        "id",
        "sku_id",
        "qty_minus_unit_price",
        "greatest_qty_unit_price",
        "warehouse_capacity",
        "last_unit_price",
        "second_first_moved_at",
        "sku_is_active",
        "sku_code",
        F.when(F.col("sku_is_active"), "yes").otherwise("no").alias("sku_is_active_label"),
        F.when(F.col("operator").isin("op_chan", "op_ann"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("warehouse_capacity").desc())
)
