from pyspark.sql import functions as F

filtered_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_add(F.col("opened_on"), 365).alias("opened_on_plus_365d"),
        F.when(F.col("city").isin("Singapore", "Bangkok"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
    )
)

line_level_moves = (
    filtered_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.capacity",
        "s.opened_on",
        "s.city_group",
        "s.wh_code_slug",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        "st.move_type",
    )
)

derived = (
    line_level_moves
    .filter(F.col("move_sku_id") >= 18)
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "city_group",
        "wh_code_slug",
        "move_id",
        "move_type",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
    )
)

enriched = (
    derived
    .filter(F.col("wh_code_slug").isNotNull())
    .select(
        "*",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug_2"),
    )
)

result = (
    enriched
    .cube("move_type", "city_group", "move_id")
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.countDistinct("city_group").alias("distinct_city_groups"),
        F.min("opened_on_month").alias("first_opened_on_month"),
        F.count("*").alias("row_count"),
    )
)
