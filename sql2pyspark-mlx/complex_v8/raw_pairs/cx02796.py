from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_qty_minus_unit_price = Window.orderBy("qty_minus_unit_price", "id").rowsBetween(-3, Window.currentRow)
w_all_by_qty_minus_unit_price_2 = Window.orderBy("qty_minus_unit_price", "id")
w_all_by_qty_minus_unit_price_3 = Window.orderBy("qty_minus_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("warehouse_id") >= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "days_since_moved_at",
        "qty_minus_unit_price",
        F.min("unit_price").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_unit_price"),
        F.round(F.avg("unit_price").over(w_all_by_qty_minus_unit_price), 2).alias("unit_price_avg_4"),
        F.dense_rank().over(w_all_by_qty_minus_unit_price_2).alias("qty_minus_unit_price_dense_rank"),
        F.nth_value("days_since_moved_at", 2).over(w_all_by_qty_minus_unit_price_3).alias("second_days_since_moved_at"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at_2"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at_3"),
        F.when(F.col("days_since_moved_at") >= 595, "A").when(F.col("days_since_moved_at") >= 175, "B").otherwise("C").alias("days_since_moved_at_tier"),
    )
)

leaders = (
    scored
    .filter(F.col("qty_minus_unit_price_dense_rank") <= 3)
    .drop("qty_minus_unit_price_dense_rank")
)

with_parent = (
    leaders.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.qty_minus_unit_price",
        "s.sku_id_warehouse_id_min_unit_price",
        "s.unit_price_avg_4",
        "s.days_since_moved_at_2",
        "s.days_since_moved_at_3",
        "s.days_since_moved_at_tier",
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

detail_rows_moves = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.qty_minus_unit_price",
        "s.days_since_moved_at_2",
        "s.days_since_moved_at_tier",
        "s.sku_unit_cost",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        "st.move_type",
    )
)

result = (
    detail_rows_moves
    .select(
        "id",
        "move_id",
        F.col("qty").alias("qty_v"),
        F.col("move_qty").alias("m_move_qty"),
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["qty_v", "m_move_qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
