from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy("unit_price", "id")
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("warehouse_id").isNotNull()))
)

joined_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.capacity") > 4912)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

windowed = (
    joined_warehouse
    .select(
        "*",
        F.ntile(10).over(w_move_type_by_unit_price).alias("unit_price_decile"),
        F.max("warehouse_capacity").over(w_warehouse_id_operator).alias("warehouse_id_operator_max_warehouse_capacity"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_id_operator), 2).alias("warehouse_capacity_vs_warehouse_id_operator_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("unit_price").alias("min_move_unit_price"))
)

profiled = (
    windowed.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.min_move_unit_price")
)

scored = (
    profiled
    .select(
        "*",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

result = (
    scored
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "warehouse_capacity",
        "sku_labels",
        "unit_price_decile",
        "warehouse_id_operator_max_warehouse_capacity",
        "warehouse_capacity_vs_warehouse_id_operator_avg",
        "min_move_unit_price",
        "moved_at_label",
        "days_since_moved_at",
        F.when(F.col("min_move_unit_price") >= 325.4, "A").when(F.col("min_move_unit_price") >= 175.7, "B").otherwise("C").alias("min_move_unit_price_tier"),
        F.substring(F.trim(F.col("moved_at_label")), 1, 3).alias("moved_at_label_prefix"),
    )
)
