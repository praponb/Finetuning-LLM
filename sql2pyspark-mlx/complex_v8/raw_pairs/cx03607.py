from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_moved_at_date_month = Window.partitionBy("operator", F.date_trunc("month", "moved_at_date"))
w_operator_sku_id_by_move_coun = Window.partitionBy("operator", "sku_id").orderBy("move_count", "id")
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w_move_type_warehouse_id_by_un_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id_by_un_3 = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price", "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.abs(F.col("qty") - 244).alias("qty_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    recent_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") == "Bangkok"), "inner")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

enriched = (
    combined_details
    .select(
        "*",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.round(F.col("min_move_unit_price") / F.nullif(F.col("warehouse_capacity"), F.lit(0)), 3).alias("min_move_unit_price_per_warehouse_capacity"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") <= 5) & (F.col("st.qty") >= 145), "left_semi")
    .filter(F.col("move_count") > 4)
)

sequenced = (
    qualified
    .select(
        "*",
        F.min("move_count").over(w_operator_moved_at_date_month).alias("operator_moved_at_date_month_min_move_count"),
        F.lead("moved_at").over(w_operator_sku_id_by_move_coun).alias("next_moved_at"),
        F.count(F.lit(1)).over(w_operator_moved_at_date_month).alias("rows_in_operator_moved_at_date_month"),
        F.max("qty_distance").over(w_move_type_warehouse_id_by_un).alias("qty_distance_max_3_prior"),
        F.sum("unit_price").over(w_move_type_warehouse_id_by_un_2).alias("running_unit_price"),
        F.lead("moved_at").over(w_move_type_warehouse_id_by_un_3).alias("next_moved_at_2"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.operator_slug",
        "s.qty_distance",
        "s.move_count",
        "s.warehouse_capacity",
        "s.moved_at_date",
        "s.operator_moved_at_date_month_min_move_count",
        "s.next_moved_at",
        "s.rows_in_operator_moved_at_date_month",
        "s.qty_distance_max_3_prior",
        "s.running_unit_price",
        "s.next_moved_at_2",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "sku_id",
        "operator_slug",
        "qty_distance",
        "move_count",
        "warehouse_capacity",
        "moved_at_date",
        "operator_moved_at_date_month_min_move_count",
        "next_moved_at",
        "rows_in_operator_moved_at_date_month",
        "qty_distance_max_3_prior",
        "running_unit_price",
        "next_moved_at_2",
        "move_id",
        "move_operator",
        "move_warehouse_id",
        "move_unit_price",
        F.date_format(F.col("next_moved_at_2"), "yyyy-MM-dd HH:00").alias("next_moved_at_2_label"),
    )
    .orderBy(F.col("rows_in_operator_moved_at_date_month").asc())
)
