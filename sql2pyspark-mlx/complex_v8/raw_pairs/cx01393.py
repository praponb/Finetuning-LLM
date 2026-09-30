from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_hour_move_id = Window.partitionBy("moved_at_hour", "move_id")
w_moved_at_month_month_by_move = Window.partitionBy(F.date_trunc("month", "moved_at_month")).orderBy(F.col("move_qty").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_hour_move_id_by_uni = Window.partitionBy("moved_at_hour", "move_id").orderBy("unit_price", "id", "move_id")
w_moved_at_month_month_by_move_2 = Window.partitionBy(F.date_trunc("month", "moved_at_month")).orderBy(F.col("move_qty").desc(), "id", "move_id").rowsBetween(-3, -1)
w_moved_at_hour_by_move_moved = Window.partitionBy("moved_at_hour").orderBy(F.col("move_moved_at").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_operator_move_id_by_mov = Window.partitionBy("move_operator", "move_id").orderBy(F.col("move_moved_at").asc_nulls_last())
w_moved_at_hour_by_qty_vs_move = Window.partitionBy("moved_at_hour").orderBy("qty_vs_moved_at_hour_move_id_avg", "id", "move_id").rowsBetween(-1, -1)
w_all_by_first_move_moved_at_t = Window.orderBy("first_move_moved_at")

cleaned = (
    df_stock_moves
    .filter((F.col("qty") < 300) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.moved_at_month",
        "s.moved_at_hour",
        "s.unit_price_plus_qty",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

exploded_detail_moves = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.moved_at_month",
        "s.moved_at_hour",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

sequenced = (
    exploded_detail_moves
    .select(
        "*",
        F.round(F.col("qty") - F.avg("qty").over(w_moved_at_hour_move_id), 2).alias("qty_vs_moved_at_hour_move_id_avg"),
        F.first("moved_at_hour").over(w_moved_at_month_month_by_move).alias("first_moved_at_hour"),
        F.ntile(5).over(w_moved_at_hour_move_id_by_uni).alias("unit_price_quintile"),
        F.round(F.avg("unit_price").over(w_moved_at_month_month_by_move_2), 2).alias("unit_price_avg_3_prior"),
        F.first("move_moved_at").over(w_moved_at_hour_by_move_moved).alias("first_move_moved_at"),
    )
)

labeled = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "moved_at_hour",
        "move_id",
        "move_sku_id",
        "move_operator",
        "move_moved_at",
        "qty_vs_moved_at_hour_move_id_avg",
        "first_moved_at_hour",
        "unit_price_quintile",
        "first_move_moved_at",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "unit_price",
        "move_id",
        "move_moved_at",
        F.rank().over(w_move_operator_move_id_by_mov).alias("move_moved_at_rank"),
        F.sum("moved_at_hour").over(w_moved_at_hour_by_qty_vs_move).alias("moved_at_hour_sum_1_prior"),
        F.dense_rank().over(w_all_by_first_move_moved_at_t).alias("first_move_moved_at_dense_rank"),
    )
)

combined_details_move = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.unit_price",
        "s.move_id",
        "s.move_moved_at_rank",
        "s.moved_at_hour_sum_1_prior",
        "s.first_move_moved_at_dense_rank",
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    combined_details_move
    .select(
        "id",
        "unit_price",
        "move_id",
        "move_moved_at_rank",
        "moved_at_hour_sum_1_prior",
        "first_move_moved_at_dense_rank",
        "move_moved_at_2",
        "move_warehouse_id",
    )
    .orderBy(F.col("move_moved_at_rank").desc())
)
