from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id_move_warehouse_id_by = Window.partitionBy("move_id", "move_warehouse_id").orderBy(F.col("qty").desc_nulls_first(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_id_by_sku_weight_kg = Window.partitionBy("move_id").orderBy("sku_weight_kg", "id", "move_id")
w_move_id_move_warehouse_id_by_2 = Window.partitionBy("move_id", "move_warehouse_id").orderBy(F.col("qty").desc_nulls_first(), "id", "move_id").rowsBetween(-1, -1)
w_move_id_warehouse_id_by_sku = Window.partitionBy("move_id", "warehouse_id").orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 290.4, "A").when(F.col("unit_price") >= 200.7, "B").otherwise("C").alias("unit_price_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

enriched = (
    valid_moves
    .filter(F.col("qty") >= 85)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "unit_price_tier",
        "move_type_slug",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.concat_ws("|", F.col("move_type"), F.col("move_type_slug")).alias("move_type_move_type_slug_label"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.unit_price_tier",
        "s.qty_filled",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

with_details = (
    exploded_detail.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.move_warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.unit_price_tier",
        "s.move_id",
        "s.move_move_type",
        "s.move_warehouse_id",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

scored = (
    with_details
    .select(
        "*",
        F.concat(F.col("unit_price_tier"), F.lit("#"), F.col("move_id").cast("string")).alias("unit_price_tier_ref"),
        F.substring(F.trim(F.col("move_move_type")), 1, 2).alias("move_move_type_prefix"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "unit_price_tier",
        "move_id",
        "move_move_type",
        "sku_is_active",
        "sku_weight_kg",
        "warehouse_wh_code",
        "unit_price_tier_ref",
        "move_move_type_prefix",
        F.first("qty", ignorenulls=True).over(w_move_id_move_warehouse_id_by).alias("first_known_qty"),
        F.row_number().over(w_move_id_by_sku_weight_kg).alias("seq_no"),
        F.max("qty").over(w_move_id_move_warehouse_id_by_2).alias("qty_max_1_prior"),
        F.first("unit_price", ignorenulls=True).over(w_move_id_move_warehouse_id_by).alias("first_known_unit_price"),
        F.count(F.lit(1)).over(w_move_id_warehouse_id_by_sku).alias("running_count"),
    )
)

tagged = (
    with_trends
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "unit_price_tier",
        "move_id",
        "move_move_type",
        "sku_is_active",
        "unit_price_tier_ref",
        "move_move_type_prefix",
        "seq_no",
        "qty_max_1_prior",
        (F.col("seq_no") - F.col("qty_max_1_prior")).alias("seq_no_minus_qty_max_1_prior"),
    )
)

result = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price_tier",
        "move_move_type",
        "sku_is_active",
        "move_move_type_prefix",
        "qty_max_1_prior",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
    )
    .orderBy("sku_id", F.col("unit_price_tier").asc_nulls_last())
)
