from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
)

joined = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.move_type_slug",
        "s.operator_prefix",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

combined_details = (
    joined.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.move_type_slug",
        "s.warehouse_wh_code",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

totals = (
    combined_details
    .groupBy("warehouse_city", "warehouse_id")
    .agg(
        F.min("warehouse_capacity").alias("min_warehouse_capacity"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum("warehouse_capacity").alias("total_warehouse_capacity"),
        F.count("*").alias("row_count"),
    )
)

result = (
    totals
    .groupingSets(
        [["warehouse_id", "row_count"], ["warehouse_id"], ["row_count"]],
        "warehouse_id", "row_count",
    )
    .agg(
        F.min("last_moved_at").alias("first_last_moved_at"),
        F.sum("row_count").alias("total_row_count"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("row_count").asc_nulls_last(),
    )
)
