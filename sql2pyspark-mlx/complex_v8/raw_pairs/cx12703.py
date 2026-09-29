from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("unit_price") > 164.1) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

with_children = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price",
        "s.move_type_ref",
        "s.qty_bucket",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

joined_sku = (
    with_children.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type_ref",
        "s.qty_bucket",
        "s.move_id",
        "s.move_sku_id",
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    joined_sku
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type_ref",
        "move_id",
        F.substring(F.trim(F.col("move_type_ref")), 1, 3).alias("move_type_ref_prefix"),
    )
    .orderBy("id", F.col("move_id").asc())
)
