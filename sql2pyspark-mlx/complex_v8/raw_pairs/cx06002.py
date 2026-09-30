from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("sku_id") >= 15)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.when(F.col("move_type").isin("OUTBOUND", "TRANSFER"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
    )
)

scored = (
    base
    .select(
        "*",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
    )
)

combined_details_warehouse = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
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
        "s.move_type_slug",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

scored_2 = (
    combined_details_warehouse
    .filter(F.col("sku_unit_cost").between(60.9, 277.0))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "warehouse_wh_code",
        "sku_code",
        "sku_unit_cost",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean_2"),
    )
)

tagged = (
    scored_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "sku_code",
        "sku_unit_cost",
        "move_type_clean",
        F.upper(F.trim(F.col("warehouse_wh_code"))).alias("warehouse_wh_code_clean"),
    )
)

move_type_stats = (
    tagged
    .groupBy("move_type")
    .agg(F.max("sku_unit_cost").alias("max_sku_unit_cost"))
)

outliers = (
    tagged.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.sku_unit_cost") == F.col("g.max_sku_unit_cost"))
    .select(
        "s.*",
        "g.max_sku_unit_cost",
        F.round(F.col("s.sku_unit_cost") - F.col("g.max_sku_unit_cost"), 2).alias("sku_unit_cost_minus_max"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    outliers.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    profiled
    .cube("sku_id", "move_count")
    .agg(
        F.round(F.sum("sku_unit_cost_minus_max"), 2).alias("total_sku_unit_cost_minus_max"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(F.col("sku_id").asc_nulls_last(), F.col("move_count").asc_nulls_last())
)
