from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("unit_price") <= 313.8) | (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"), F.sum("qty").alias("total_move_qty"))
)

with_stats = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
    )
)

enriched = (
    with_stats.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category").isin("Toys", "Beauty"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.greatest_unit_price_qty",
        "s.move_count",
        "s.total_move_qty",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

exploded_detail_moves = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.move_count",
        "s.sku_code",
        "s.sku_unit_cost",
        "s.sku_category",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
    )
)

annotated = (
    exploded_detail_moves
    .select(
        "*",
        F.concat_ws(" / ", F.col("sku_category"), F.col("sku_code")).alias("sku_category_sku_code_label"),
        (F.col("move_count") - F.col("sku_unit_cost")).alias("move_count_minus_sku_unit_cost"),
    )
)

result = (
    annotated
    .groupingSets(
        [["sku_category", "warehouse_id"], ["sku_category"], ["warehouse_id"], []],
        "sku_category", "warehouse_id",
    )
    .agg(
        F.count(F.when(F.col("move_type") != "INBOUND", 1)).alias("n_move_type_inbound"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("sku_category").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
