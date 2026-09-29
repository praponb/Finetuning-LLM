from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_move_count_full = Window.orderBy(F.col("move_count").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_operator_or_unknown = Window.partitionBy("operator", "operator_or_unknown")
w_all_by_distinct_warehouse_id = Window.orderBy("distinct_warehouse_id_in_operator_operator_or_un", "id")
w_move_count_second_move_count = Window.partitionBy("move_count", "second_move_count")
w_operator_or_unknown_by_least = Window.partitionBy("operator_or_unknown").orderBy("least_unit_price_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 60.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

joined_warehouse = (
    with_counts.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") == "Singapore"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_windows = (
    joined_warehouse
    .select(
        "*",
        F.nth_value("move_count", 2).over(w_all_by_move_count_full).alias("second_move_count"),
        F.size(F.collect_set("warehouse_id").over(w_operator_operator_or_unknown)).alias("distinct_warehouse_id_in_operator_operator_or_un"),
        F.first("sku_weight_kg").over(w_all_by_move_count_full).alias("first_sku_weight_kg"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        (F.col("first_sku_weight_kg") - F.lag("first_sku_weight_kg").over(w_all_by_distinct_warehouse_id)).alias("first_sku_weight_kg_change"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_count_second_move_count), F.lit(0)), 4).alias("qty_share_of_move_count_second_move_count"),
        F.count(F.lit(1)).over(w_operator_or_unknown_by_least).alias("running_count"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_warehouse_id_in_operator_operator_or_un") > 3)
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "unit_price",
        "operator_or_unknown",
        "least_unit_price_qty",
        "operator_move_type_label",
        "warehouse_capacity",
        "sku_weight_kg",
        "distinct_warehouse_id_in_operator_operator_or_un",
        "first_sku_weight_kg",
        "first_sku_weight_kg_change",
        "running_count",
    )
)
