from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_by_unit_price = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_warehouse_opened_o = Window.partitionBy("sku_id").orderBy("warehouse_opened_on", "id")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("unit_price").desc(), "id")
w_moved_at_month_by_unit_price_2 = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_rn_sku_id_by_prev_moved_at = Window.partitionBy("rn", "sku_id").orderBy("prev_moved_at", "id")
w_warehouse_opened_on_decile = Window.partitionBy("warehouse_opened_on_decile")
w_warehouse_opened_on_decile_p = Window.partitionBy("warehouse_opened_on_decile", F.date_trunc("month", "prev_moved_at")).orderBy(F.col("prev_moved_at").desc(), "id")
w_rn_sku_id_by_prev_moved_at_t = Window.partitionBy("rn", "sku_id").orderBy("prev_moved_at")

scoped = (
    df_stock_moves
    .filter(F.col("qty") >= 154)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") < 11768) & (F.col("w.opened_on") < F.lit("2025-07-03").cast("date")), "left_anti")
)

joined_warehouse = (
    qualified.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_group",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

without_matches = (
    joined_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_bee") & (F.col("st.qty") < 288), "left_anti")
)

with_windows = (
    without_matches
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "warehouse_opened_on",
        F.last("qty", ignorenulls=True).over(w_moved_at_month_by_unit_price).alias("last_known_qty"),
        F.ntile(10).over(w_sku_id_by_warehouse_opened_o).alias("warehouse_opened_on_decile"),
        F.row_number().over(w_sku_id_by_warehouse_opened_o).alias("rn"),
        F.round(F.cume_dist().over(w_warehouse_id_moved_at_month), 3).alias("unit_price_cume_dist"),
        F.round(F.cume_dist().over(w_sku_id_by_warehouse_opened_o), 3).alias("warehouse_opened_on_cume_dist"),
        F.lag("moved_at").over(w_moved_at_month_by_unit_price_2).alias("prev_moved_at"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "sku_id",
        "qty",
        "last_known_qty",
        "warehouse_opened_on_decile",
        "rn",
        "unit_price_cume_dist",
        "prev_moved_at",
        (F.col("warehouse_opened_on_decile") - F.lag("warehouse_opened_on_decile").over(w_rn_sku_id_by_prev_moved_at)).alias("warehouse_opened_on_decile_change"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_decile).alias("rows_in_warehouse_opened_on_decile"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_warehouse_opened_on_decile_p)).alias("days_since_prev_warehouse_opened_on"),
        F.lead("qty", 2).over(w_warehouse_opened_on_decile_p).alias("next2_qty"),
        F.round(F.cume_dist().over(w_rn_sku_id_by_prev_moved_at_t), 3).alias("prev_moved_at_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("prev_moved_at_cume_dist").alias("m_prev_moved_at_cume_dist"),
        F.col("unit_price_cume_dist").alias("unit_price_cume_dist_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_prev_moved_at_cume_dist", "unit_price_cume_dist_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
