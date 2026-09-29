from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc(), "id", "move_id")
w_warehouse_id_sku_id_by_unit_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_unit_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc())

filtered = (
    df_stock_moves
    .filter(F.col("operator").isin("op_eve", "op_bee", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_children = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 17), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

windowed = (
    with_children
    .select(
        "*",
        F.row_number().over(w_warehouse_id_sku_id_by_unit).alias("unit_price_seq"),
        F.min("qty").over(w_warehouse_id_sku_id_by_unit_2).alias("min_qty_so_far"),
        F.rank().over(w_warehouse_id_sku_id_by_unit_3).alias("unit_price_rank"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_id",
        "move_moved_at",
        "move_qty",
        "unit_price_seq",
        "min_qty_so_far",
        "unit_price_rank",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        F.hour(F.col("move_moved_at")).alias("move_moved_at_hour"),
    )
)

result = (
    annotated
    .select(
        "id",
        "move_id",
        "move_moved_at_hour",
        "unit_price_seq",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_moved_at_hour", "unit_price_seq"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
