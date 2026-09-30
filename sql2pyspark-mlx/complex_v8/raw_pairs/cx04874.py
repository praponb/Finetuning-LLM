from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.year(F.col("opened_on")) == 2025) & (F.lower(F.col("wh_code")).like("%bk%")))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
    )
)

with_children_moves = (
    cleaned_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.city",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    with_children_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.total_move_unit_price", "st.last_moved_at")
)

result = (
    with_stats
    .groupingSets(
        [["move_id", "move_sku_id"], ["move_id"], ["move_sku_id"], []],
        "move_id", "move_sku_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("city") == "Bangkok", 1)) / F.count("*"), 1).alias("pct_city_bangkok"),
    )
    .orderBy(
        F.col("move_id").asc_nulls_last(),
        F.col("move_sku_id").asc_nulls_last(),
    )
)
