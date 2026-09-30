from pyspark.sql import functions as F

recent = (
    df_skus
    .filter(F.size(F.col("labels")) >= 2)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

exploded_detail_moves = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.category",
        "s.unit_cost",
        "s.weight_kg",
        "s.labels",
        "s.is_active",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

exploded = (
    exploded_detail_moves
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        "move_id",
        "move_qty",
        "move_moved_at",
        F.explode("labels").alias("label"),
    )
)

scored = (
    exploded
    .filter(F.col("unit_cost") <= 277.0)
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "move_id",
        "move_qty",
        "label",
        F.substring(F.trim(F.col("label")), 1, 3).alias("label_prefix"),
        F.coalesce(F.col("move_qty"), F.lit(0)).alias("move_qty_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "move_id",
        "move_qty",
        "move_qty_filled",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_qty", "move_qty_filled"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
