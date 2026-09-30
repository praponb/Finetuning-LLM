from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")
w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_city_move_type_by = Window.partitionBy("warehouse_city", "move_type").orderBy("moved_at")
w_all_by_qty = Window.orderBy("qty", "id")
w_warehouse_city_move_type_by_2 = Window.partitionBy("warehouse_city", "move_type").orderBy("moved_at", "id")
w_all_by_minutes_since_prev_mo = Window.orderBy(F.col("minutes_since_prev_moved_at").asc_nulls_last(), "id")
w_seq_no_by_qty = Window.partitionBy("seq_no").orderBy(F.col("qty").desc(), "id")
w_seq_no_by_qty_ties = Window.partitionBy("seq_no").orderBy(F.col("qty").desc())
w_all_by_minutes_since_prev_mo_2 = Window.orderBy(F.col("minutes_since_prev_moved_at").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

base = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
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

with_details_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city").isin("Bangkok", "Kuala Lumpur"))
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

metrics = (
    with_details_warehouse
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator), F.lit(0)), 4).alias("qty_share_of_operator"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_by_qty))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.lead("moved_at", 2).over(w_warehouse_id_by_unit_price).alias("next2_moved_at"),
        F.nth_value("qty", 2).over(w_warehouse_id_by_qty_full).alias("second_qty"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_city), 2).alias("unit_price_vs_warehouse_city_avg"),
        F.round(F.percent_rank().over(w_warehouse_city_move_type_by), 3).alias("moved_at_percent_rank"),
        F.ntile(10).over(w_all_by_qty).alias("qty_decile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_city), F.lit(0)), 4).alias("unit_price_share_of_warehouse_city"),
        F.row_number().over(w_warehouse_city_move_type_by_2).alias("seq_no"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "warehouse_city",
        "sku_labels",
        "qty_share_of_operator",
        "next2_moved_at",
        "unit_price_vs_warehouse_city_avg",
        "moved_at_percent_rank",
        "qty_decile",
        "seq_no",
        F.lead("minutes_since_prev_moved_at").over(w_all_by_minutes_since_prev_mo).alias("next_minutes_since_prev_moved_at"),
        F.row_number().over(w_seq_no_by_qty).alias("seq_no_2"),
        F.round(F.cume_dist().over(w_seq_no_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.avg("qty_decile").over(w_all_by_minutes_since_prev_mo_2), 2).alias("qty_decile_avg_4"),
    )
)

result = (
    ranked
    .filter(F.col("qty") >= 169)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "sku_labels",
        "qty_share_of_operator",
        "next2_moved_at",
        "unit_price_vs_warehouse_city_avg",
        "moved_at_percent_rank",
        "qty_decile",
        "next_minutes_since_prev_moved_at",
        "seq_no_2",
        "qty_decile_avg_4",
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
    .orderBy(F.col("operator").desc())
)
