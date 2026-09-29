from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_operator_warehouse_city_by_q = Window.partitionBy("operator", "warehouse_city").orderBy(F.col("qty").desc(), "id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_all_by_moved_at_prior_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_moved_at_moving_6 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_chan")
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

enriched_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
    )
)

windowed = (
    enriched_warehouse
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "warehouse_city",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_moved_at_month), F.lit(0)), 4).alias("unit_price_share_of_moved_at_month"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_month), 2).alias("unit_price_vs_moved_at_month_avg"),
        F.lead("unit_price", 1, 0).over(w_all_by_moved_at).alias("next_unit_price"),
        F.ntile(5).over(w_operator_warehouse_city_by_q).alias("qty_quintile"),
        F.ntile(10).over(w_warehouse_id_by_qty).alias("qty_decile"),
        F.lead("unit_price", 2).over(w_all_by_moved_at).alias("next2_unit_price"),
        F.ntile(10).over(w_all_by_moved_at).alias("moved_at_decile"),
        F.round(F.col("qty") - F.avg("qty").over(w_moved_at_month), 2).alias("qty_vs_moved_at_month_avg"),
        F.round(F.avg("qty").over(w_moved_at_month), 2).alias("moved_at_month_avg_qty"),
        F.round(F.avg("qty").over(w_all_by_moved_at_prior_2), 2).alias("qty_avg_2_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_month), F.lit(0)), 4).alias("qty_share_of_moved_at_month"),
        F.max("qty").over(w_all_by_moved_at_moving_6).alias("qty_max_7"),
    )
)

first_rows = windowed.filter((F.col("rn") <= 5) & (F.col("qty_decile") < 8))

result = (
    first_rows
    .filter(F.col("next_unit_price") <= 363.6)
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "warehouse_city",
        "unit_price_share_of_moved_at_month",
        "rn",
        "unit_price_vs_moved_at_month_avg",
        "next_unit_price",
        "qty_decile",
        "moved_at_decile",
        "qty_vs_moved_at_month_avg",
        "moved_at_month_avg_qty",
        "qty_avg_2_prior",
        "qty_max_7",
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_city")), " ", "_")).alias("warehouse_city_slug"),
    )
    .orderBy(
        F.col("unit_price_share_of_moved_at_month").desc(),
        "unit_price_vs_moved_at_month_avg",
    )
)
