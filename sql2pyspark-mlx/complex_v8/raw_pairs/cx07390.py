from pyspark.sql import functions as F

recent = (
    df_warehouses
    .filter(F.col("capacity") < 11506)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price").between(73.0, 367.7)), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.opened_on",
        "s.days_since_opened_on",
        "s.city_or_unknown",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

scored = (
    exploded_detail
    .filter(F.col("days_since_opened_on").between(140, 665))
    .select(
        "*",
        F.length(F.trim(F.col("city"))).alias("city_len"),
        F.coalesce(F.col("city_or_unknown"), F.lit("UNKNOWN")).alias("city_or_unknown_or_unknown"),
        F.when(F.col("city").isin("Bangkok", "Kuala Lumpur"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
    )
)

city_or_unknown_or_unknown_stats = (
    scored
    .groupBy("city_or_unknown_or_unknown")
    .agg(F.max("days_since_opened_on").alias("max_days_since_opened_on"))
)

compared = (
    scored.alias("s")
    .join(city_or_unknown_or_unknown_stats.alias("g"), F.col("g.city_or_unknown_or_unknown") == F.col("s.city_or_unknown_or_unknown"), "inner")
    .filter(F.col("s.days_since_opened_on") == F.col("g.max_days_since_opened_on"))
    .select(
        "s.*",
        "g.max_days_since_opened_on",
        F.round(F.col("s.days_since_opened_on") - F.col("g.max_days_since_opened_on"), 2).alias("days_since_opened_on_minus_max"),
    )
)

result = (
    compared
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "city_or_unknown",
        "move_id",
        "move_moved_at",
        "move_unit_price",
        "city_len",
        "city_or_unknown_or_unknown",
        "city_group",
        "max_days_since_opened_on",
        "days_since_opened_on_minus_max",
        F.greatest(F.col("days_since_opened_on"), F.col("move_unit_price")).alias("greatest_days_since_opened_on_move_unit_price"),
    )
    .orderBy(F.col("city_or_unknown_or_unknown").desc())
)
