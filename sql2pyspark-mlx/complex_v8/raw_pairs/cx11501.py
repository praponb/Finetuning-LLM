from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_by_move_coun = Window.partitionBy("sku_id", "operator").orderBy(F.col("move_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator = Window.partitionBy("operator")
w_sku_id_operator_by_move_coun_2 = Window.partitionBy("sku_id", "operator").orderBy(F.col("move_count").asc_nulls_last(), "id")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_second_move_count_move_move = Window.partitionBy("second_move_count", "move_move_type_count").orderBy("move_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("operator").isin("op_dew", "op_eve", "op_bee")))
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

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("unit_price").alias("max_move_unit_price"),
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.max_move_unit_price",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

metrics = (
    with_stats
    .select(
        "*",
        F.nth_value("move_count", 2).over(w_sku_id_operator_by_move_coun).alias("second_move_count"),
        F.size(F.collect_set("warehouse_id").over(w_operator)).alias("distinct_warehouse_id_in_operator"),
        F.lead("unit_price").over(w_sku_id_operator_by_move_coun_2).alias("next_unit_price"),
        F.max("unit_price").over(w_operator).alias("operator_max_unit_price"),
    )
)

combined_details = (
    metrics.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "max_move_unit_price",
        "move_move_type_count",
        "distinct_warehouse_id_in_operator",
        "next_unit_price",
        "operator_max_unit_price",
        "warehouse_city",
        "warehouse_capacity",
        F.min("move_move_type_count").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_move_move_type_count"),
        F.count(F.lit(1)).over(w_second_move_count_move_move).alias("running_count"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.max_move_unit_price",
        "s.move_move_type_count",
        "s.distinct_warehouse_id_in_operator",
        "s.next_unit_price",
        "s.operator_max_unit_price",
        "s.move_type_warehouse_id_min_move_move_type_count",
        "s.running_count",
        F.col("w.capacity").alias("warehouse_capacity_2"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "max_move_unit_price",
        "move_move_type_count",
        "distinct_warehouse_id_in_operator",
        "next_unit_price",
        "move_type_warehouse_id_min_move_move_type_count",
        "running_count",
        "warehouse_capacity_2",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.col("move_move_type_count") - F.col("max_move_unit_price")).alias("move_move_type_count_minus_max_move_unit_price"),
    )
    .orderBy(F.col("distinct_warehouse_id_in_operator").desc())
)
