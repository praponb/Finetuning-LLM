from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty = Window.orderBy("qty", "id")
w_warehouse_id_sku_id_by_qty_t = Window.partitionBy("warehouse_id", "sku_id").orderBy("qty")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.to_date(F.col("moved_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
        F.when(F.col("operator").isin("op_eve", "op_dew"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
)

with_windows = (
    cleaned_moves
    .select(
        "*",
        F.lead("qty").over(w_all_by_qty).alias("next_qty"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_qty))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.lag("qty").over(w_all_by_qty).alias("prev_qty"),
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_qty_t), 3).alias("qty_cume_dist"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.total_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    with_stats
    .select("id", "total_move_unit_price", "qty_cume_dist")
    .unpivot(
        ids=["id"],
        values=["total_move_unit_price", "qty_cume_dist"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
