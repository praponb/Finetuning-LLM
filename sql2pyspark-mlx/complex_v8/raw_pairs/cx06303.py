from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

combined_details_sku = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator_prefix",
        "s.operator_move_type_label",
        "s.move_type_or_unknown",
        "sk.sku_code",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

enriched = (
    combined_details_sku
    .filter(F.col("warehouse_opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "operator_prefix",
        "operator_move_type_label",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.substring(F.trim(F.col("move_type_or_unknown")), 1, 4).alias("move_type_or_unknown_prefix"),
    )
)

labeled = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "operator_move_type_label",
        "moved_at_label",
        "move_type_or_unknown_prefix",
        F.substring(F.trim(F.col("operator_prefix")), 1, 3).alias("operator_prefix_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("moved_at_label")), " ", "_")).alias("moved_at_label_slug"),
        F.substring(F.trim(F.col("operator_prefix")), 1, 4).alias("operator_prefix_prefix_2"),
    )
)

result = (
    labeled
    .filter(F.col("operator_prefix_prefix").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type_or_unknown_prefix",
        "moved_at_label_slug",
        "operator_prefix_prefix_2",
        F.length(F.trim(F.col("operator_prefix_prefix_2"))).alias("operator_prefix_prefix_2_len"),
        F.concat(F.col("moved_at_label_slug"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("moved_at_label_slug_ref"),
    )
    .orderBy(
        F.col("move_type_or_unknown_prefix").desc(),
        F.col("moved_at_label_slug").desc(),
        "id",
    )
    .limit(5)
)
