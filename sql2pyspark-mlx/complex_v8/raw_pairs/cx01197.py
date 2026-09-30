from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("qty") > 102)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

derived = (
    scoped_moves
    .filter(F.col("move_type") != "TRANSFER")
    .select(
        "*",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

scored = (
    derived
    .filter(F.col("move_type").isin("ADJUST", "INBOUND", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.when(F.col("move_type").isin("INBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("B%")) & (F.col("w.opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date"))), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["sku_id", "move_type_group"], ["sku_id"], []],
        "sku_id", "move_type_group",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("n_move_type_is_not_null"),
    )
    .select(
        "sku_id",
        F.coalesce(F.col("move_type_group"), F.lit("ALL")).alias("move_type_group"),
        "gid",
        "row_count",
        "n_move_type_is_not_null",
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("move_type_group").asc_nulls_last(),
    )
)
