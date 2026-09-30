from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_sku_id_by_qty_running = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter((F.col("operator") == "op_eve") | (F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

enriched = (
    recent
    .filter(F.col("unit_price") >= 160.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_filled",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "qty_filled",
        "move_type_slug",
        F.round(F.avg("qty_filled").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_qty_filled"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_sku_id)).alias("distinct_move_type_in_warehouse_id_sku_id"),
        F.sum("qty_filled").over(w_sku_id_by_qty_running).alias("running_qty_filled"),
        F.sum("qty").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_sum_qty"),
    )
)

exploded_detail_moves = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") >= 98), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.qty_filled",
        "s.move_type_slug",
        "s.warehouse_id_sku_id_avg_qty_filled",
        "s.distinct_move_type_in_warehouse_id_sku_id",
        "s.running_qty_filled",
        "s.warehouse_id_sku_id_sum_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    exploded_detail_moves
    .rollup("move_type", "move_move_type")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("moved_at").alias("last_moved_at"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        F.coalesce(F.col("move_move_type"), F.lit("ALL")).alias("move_move_type"),
        "grouping_level",
        "last_moved_at",
    )
)
