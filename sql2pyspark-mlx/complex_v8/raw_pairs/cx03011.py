from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_is_active = Window.partitionBy("warehouse_id", "sku_is_active")
w_all_by_qty_ties = Window.orderBy("qty")

scoped_moves = (
    df_stock_moves
    .filter(F.col("qty") <= 326)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

without_matches = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") >= 4912) & (F.upper(F.trim(F.col("w.wh_code"))).like("B%")), "left_semi")
)

with_details_sku = (
    without_matches.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("E%")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

sequenced = (
    with_details_sku
    .select(
        "*",
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_sku_is_active), 2).alias("qty_vs_warehouse_id_sku_is_active_avg"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_is_active).alias("rows_in_warehouse_id_sku_is_active"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_is_active), 2).alias("warehouse_id_sku_is_active_avg_qty"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.sku_labels",
        "s.qty_vs_warehouse_id_sku_is_active_avg",
        "s.rows_in_warehouse_id_sku_is_active",
        "s.qty_cume_dist",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

without_matches_2 = (
    with_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_bee", "op_eve")), "left_anti")
)

result = (
    without_matches_2
    .groupBy("sku_id")
    .pivot("operator", ["op_ann", "op_bee", "op_dew", "op_eve"])
    .agg(F.min("qty_cume_dist"))
    .orderBy("sku_id")
)
