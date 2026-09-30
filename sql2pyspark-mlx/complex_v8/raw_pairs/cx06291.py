from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

tagged = (
    prepared_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "move_type_slug",
        "unit_price_bucket",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat(F.col("move_type_slug"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_slug_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug_2"),
    )
)

result = (
    tagged
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price_bucket",
        "move_type_slug_ref",
        "move_type_slug_2",
        F.length(F.trim(F.col("move_type_slug_ref"))).alias("move_type_slug_ref_len"),
        F.lower(F.trim(F.col("move_type_operator_label"))).alias("move_type_operator_label_clean"),
    )
    .orderBy("move_type_slug_2")
)
