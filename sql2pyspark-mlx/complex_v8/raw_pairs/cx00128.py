from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .filter(F.col("unit_price") < 289.7)
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

latest = (
    prepared
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

line_level_moves = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isin("TRANSFER", "ADJUST", "OUTBOUND")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

screened = (
    line_level_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("move_unit_price").between(99.1, 298.9))
)

result = (
    screened
    .groupingSets(
        [["operator", "move_id"], ["operator"], ["move_id"], []],
        "operator", "move_id",
    )
    .agg(
        F.round(F.sum("move_unit_price") / F.nullif(F.sum("move_qty"), F.lit(0)), 3).alias("move_unit_price_to_move_qty_ratio"),
    )
    .select(
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "move_id",
        "move_unit_price_to_move_qty_ratio",
    )
    .orderBy(F.col("operator").asc_nulls_last(), F.col("move_id").asc_nulls_last())
)
