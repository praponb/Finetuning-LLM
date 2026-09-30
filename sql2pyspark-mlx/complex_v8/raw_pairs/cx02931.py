from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count = Window.partitionBy("move_count")
w_all_by_warehouse_capacity_ru = Window.orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_warehouse_capacity = Window.orderBy("warehouse_capacity", "id")
w_move_type_by_move_count_2_pr = Window.partitionBy("move_type").orderBy("move_count_2", "id").rowsBetween(-2, -1)

scoped = df_stock_moves.filter((F.col("operator").isNotNull()) & (F.col("qty").between(102, 357)))

screened = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("B%")) & (F.col("w.capacity") > 4912), "left_semi")
)

combined_details = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.wh_code").isNotNull()), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_details_warehouse = (
    combined_details.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.warehouse_capacity",
        F.col("w.capacity").alias("warehouse_capacity_2"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

screened_2 = (
    with_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 3), "left_semi")
    .filter(F.col("warehouse_city") != "Singapore")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

summarized_rows = (
    screened_2.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

with_windows = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "warehouse_capacity",
        "warehouse_capacity_2",
        "warehouse_city",
        "warehouse_opened_on",
        "move_count",
        "move_count_2",
        F.size(F.collect_set("move_type").over(w_move_count)).alias("distinct_move_type_in_move_count"),
        F.min("move_count_2").over(w_all_by_warehouse_capacity_ru).alias("min_move_count_2_so_far"),
        F.lead("warehouse_capacity").over(w_all_by_warehouse_capacity).alias("next_warehouse_capacity"),
        F.size(F.collect_set("warehouse_id").over(w_move_count)).alias("distinct_warehouse_id_in_move_count"),
        F.max("move_count").over(w_move_type_by_move_count_2_pr).alias("move_count_max_2_prior"),
        F.sum("move_count").over(w_all_by_warehouse_capacity_ru).alias("running_move_count"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "warehouse_capacity",
        "warehouse_capacity_2",
        "warehouse_city",
        "move_count",
        "move_count_2",
        "distinct_move_type_in_move_count",
        "min_move_count_2_so_far",
        "distinct_warehouse_id_in_move_count",
        "move_count_max_2_prior",
    )
    .orderBy("sku_id")
)
