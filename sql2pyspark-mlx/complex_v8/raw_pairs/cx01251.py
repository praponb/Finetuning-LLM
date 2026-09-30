from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("sku_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_details = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_minus_qty",
        "s.moved_at_label",
        "s.moved_at_date",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

filtered = (
    with_details
    .crossJoin(with_details.agg(F.avg("unit_price").alias("overall_avg_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_avg_unit_price"))
    .drop("overall_avg_unit_price")
)

detail_rows = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        "s.moved_at_date",
        "s.sku_is_active",
        "s.sku_category",
        "s.sku_labels",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

joined = (
    detail_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.unit_cost") > 38.3)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.moved_at_label",
        "s.sku_is_active",
        "s.sku_category",
        "s.sku_labels",
        "s.move_id",
        "s.move_move_type",
        "s.move_operator",
        "s.move_unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category_2"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

sku_category_stats = (
    joined
    .groupBy("sku_category")
    .agg(F.avg("sku_weight_kg").alias("avg_sku_weight_kg"))
)

vs_group = (
    joined.alias("s")
    .join(sku_category_stats.alias("g"), F.col("g.sku_category") == F.col("s.sku_category"), "inner")
    .filter(F.col("s.sku_weight_kg") > F.col("g.avg_sku_weight_kg"))
    .select(
        "s.*",
        "g.avg_sku_weight_kg",
        F.round(F.col("s.sku_weight_kg") - F.col("g.avg_sku_weight_kg"), 2).alias("sku_weight_kg_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at_label",
        "sku_is_active",
        "sku_labels",
        "move_id",
        "move_move_type",
        "move_operator",
        "move_unit_price",
        "sku_unit_cost",
        "sku_category_2",
        "sku_weight_kg_minus_avg",
    )
    .orderBy("sku_category_2")
)
