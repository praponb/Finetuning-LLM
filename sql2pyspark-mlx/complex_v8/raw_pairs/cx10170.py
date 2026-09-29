from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

derived = (
    base_moves
    .select(
        "*",
        (F.col("operator_len") - F.col("qty")).alias("operator_len_minus_qty"),
        F.lower(F.trim(F.col("operator_or_unknown"))).alias("operator_or_unknown_clean"),
    )
)

enriched_sku = (
    derived.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_len",
        "s.operator_len_minus_qty",
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

exploded = (
    enriched_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "operator_len",
        "operator_len_minus_qty",
        "sku_code",
        "sku_is_active",
        F.explode("sku_labels").alias("sku_label"),
    )
)

labeled = (
    exploded
    .select(
        "*",
        F.substring(F.trim(F.col("sku_label")), 1, 3).alias("sku_label_prefix"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "operator_len",
        "operator_len_minus_qty",
        "sku_code",
        "sku_label",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("days_since_moved_at").desc())
)
