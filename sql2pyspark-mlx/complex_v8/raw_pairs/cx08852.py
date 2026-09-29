from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

stats = (
    valid_moves
    .groupBy("sku_id", "operator")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.max("unit_price").alias("max_unit_price"),
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("n_move_type_inbound"),
    )
)

with_parent_sku = (
    stats.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.sku_id",
        "s.operator",
        "s.min_unit_price",
        "s.max_unit_price",
        "s.n_move_type_inbound",
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_details_sku = (
    with_parent_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.is_active"))
    .select(
        "s.sku_id",
        "s.operator",
        "s.min_unit_price",
        "s.n_move_type_inbound",
        "s.sku_labels",
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_activity = (
    with_details_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") <= 337.6), "left_anti")
)

result = (
    with_activity
    .select(
        "sku_id",
        "operator",
        "min_unit_price",
        "n_move_type_inbound",
        "sku_code",
    )
    .orderBy(
        F.col("sku_code").asc_nulls_last(),
        F.col("n_move_type_inbound").asc_nulls_last(),
    )
)
