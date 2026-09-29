from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_moved_at_to_date_sku_id = Window.partitionBy(F.to_date("move_moved_at"), "sku_id").orderBy(F.col("move_moved_at").desc_nulls_first(), "move_type", "sku_id", "move_id").rowsBetween(-2, -1)
w_move_moved_at_to_date_sku_id_2 = Window.partitionBy(F.to_date("move_moved_at"), "sku_id").orderBy(F.col("move_moved_at").desc_nulls_first(), "move_type", "sku_id", "move_id")
w_move_moved_at_month_by_stdde = Window.partitionBy(F.month("move_moved_at")).orderBy(F.col("stddev_unit_price_minus_max").asc_nulls_last())

recent_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_eve", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

warehouse_id_stats = (
    recent_moves
    .groupBy("warehouse_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

above_average = (
    recent_moves.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price") == F.col("g.max_unit_price"))
    .select(
        "s.*",
        "g.max_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.max_unit_price"), 2).alias("unit_price_minus_max"),
    )
)

summary = (
    above_average
    .filter(F.col("max_unit_price") < 325.4)
    .groupBy("move_type", "sku_id")
    .agg(
        F.round(F.stddev("unit_price_minus_max"), 2).alias("stddev_unit_price_minus_max"),
        F.round(100 * F.count(F.when(F.col("move_type") == "ADJUST", 1)) / F.count("*"), 1).alias("pct_move_type_adjust"),
    )
)

screened = (
    summary.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 387.1), "left_semi")
)

detail_rows_moves = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()), "left")
    .select(
        "s.move_type",
        "s.sku_id",
        "s.stddev_unit_price_minus_max",
        "s.pct_move_type_adjust",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

windowed = (
    detail_rows_moves
    .select(
        "*",
        F.max("pct_move_type_adjust").over(w_move_moved_at_to_date_sku_id).alias("pct_move_type_adjust_max_2_prior"),
        F.round((F.unix_timestamp("move_moved_at") - F.unix_timestamp(F.lag("move_moved_at").over(w_move_moved_at_to_date_sku_id_2))) / 60, 1).alias("minutes_since_prev_move_moved_at"),
        F.dense_rank().over(w_move_moved_at_month_by_stdde).alias("stddev_unit_price_minus_max_dense_rank"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.move_warehouse_id")) & (F.col("w.capacity") > 4912), "left_semi")
)

result = (
    filtered
    .select(
        "move_type",
        "sku_id",
        F.col("pct_move_type_adjust_max_2_prior").alias("m_pct_move_type_adjust_max_2_prior"),
        F.col("minutes_since_prev_move_moved_at").alias("minutes_since_prev_move_moved_at_v"),
    )
    .unpivot(
        ids=["move_type", "sku_id"],
        values=["m_pct_move_type_adjust_max_2_prior", "minutes_since_prev_move_moved_at_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
