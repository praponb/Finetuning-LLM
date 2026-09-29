from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.hour(F.col("st.moved_at")).between(8, 10)) & (F.col("st.unit_price") <= 323.9), "left_anti")
)

with_details = (
    with_activity.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.col("sk.category").isin("Toys", "Electronics")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.operator_prefix",
        "s.move_type_slug",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_details
    .filter(F.array_contains(F.col("sku_labels"), "express"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "operator_prefix",
        "move_type_slug",
        "sku_code",
        "sku_labels",
        F.concat_ws("-", F.col("sku_code"), F.col("move_type_slug")).alias("sku_code_move_type_slug_label"),
        F.concat_ws(" / ", F.col("operator"), F.col("operator_prefix")).alias("operator_operator_prefix_label"),
    )
    .orderBy(
        F.col("operator_operator_prefix_label").asc(),
        F.col("operator_prefix").desc(),
        "id",
    )
    .limit(5)
)
