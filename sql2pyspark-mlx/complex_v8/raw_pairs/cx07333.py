from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_opened_on = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id")
w_city = Window.partitionBy("city")
w_city_by_capacity = Window.partitionBy("city").orderBy(F.col("capacity").desc(), "id")
w_city_by_opened_on_prior_1 = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(-1, -1)
w_capacity_tercile_by_opened_o = Window.partitionBy("capacity_tercile").orderBy("opened_on", "id").rowsBetween(-1, Window.currentRow)
w_city_capacity_tercile = Window.partitionBy("city", "capacity_tercile")

recent = df_warehouses.filter(F.col("capacity") < 11506)

with_trends = (
    recent
    .select(
        "*",
        F.lag("capacity", 1, 0).over(w_city_by_opened_on).alias("prev_capacity"),
        F.ntile(3).over(w_city_by_capacity).alias("capacity_tercile"),
        F.round(F.avg("capacity").over(w_city), 2).alias("city_avg_capacity"),
        F.round(F.avg("capacity").over(w_city_by_opened_on_prior_1), 2).alias("capacity_avg_1_prior"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.max("capacity_tercile").over(w_capacity_tercile_by_opened_o).alias("capacity_tercile_max_2"),
        F.round(F.col("capacity_tercile") - F.avg("capacity_tercile").over(w_city_capacity_tercile), 2).alias("capacity_tercile_vs_city_capacity_tercile_avg"),
    )
)

line_level_moves = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        "s.capacity_tercile",
        "s.city_avg_capacity",
        "s.capacity_avg_1_prior",
        "s.capacity_tercile_max_2",
        "s.capacity_tercile_vs_city_capacity_tercile_avg",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_stats = (
    line_level_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.total_move_qty", "st.first_moved_at")
)

enriched = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        "s.city_avg_capacity",
        "s.capacity_avg_1_prior",
        "s.capacity_tercile_max_2",
        "s.capacity_tercile_vs_city_capacity_tercile_avg",
        "s.move_id",
        "s.move_type",
        "s.move_moved_at",
        "s.first_moved_at",
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator").isin("op_dew", "op_bee", "op_eve")) & (F.col("st.qty") > 198), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "capacity",
        "city_avg_capacity",
        "capacity_avg_1_prior",
        "move_id",
        "move_moved_at",
        "first_moved_at",
        "move_unit_price",
        "move_qty",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
