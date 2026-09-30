from pyspark.sql import functions as F

base = (
    df_warehouses
    .filter((F.col("capacity") >= 2614) & (~F.col("city").isin("Kuala Lumpur", "Bangkok")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.initcap(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
)

line_level = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.capacity",
        "s.wh_code_clean",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

derived = (
    line_level
    .select(
        "*",
        F.date_trunc("day", F.col("move_moved_at")).alias("move_moved_at_day"),
        F.length(F.trim(F.col("wh_code_clean"))).alias("wh_code_clean_len"),
    )
)

move_operator_stats = (
    derived
    .groupBy("move_operator")
    .agg(F.avg("move_unit_price").alias("avg_move_unit_price"))
)

compared = (
    derived.alias("s")
    .join(move_operator_stats.alias("g"), F.col("g.move_operator") == F.col("s.move_operator"), "inner")
    .filter(F.col("s.move_unit_price") > F.col("g.avg_move_unit_price"))
    .select(
        "s.*",
        "g.avg_move_unit_price",
        F.round(F.col("s.move_unit_price") - F.col("g.avg_move_unit_price"), 2).alias("move_unit_price_minus_avg"),
    )
)

tagged = (
    compared
    .select(
        "*",
        (F.floor(F.col("avg_move_unit_price") / 100) * 100).alias("avg_move_unit_price_bucket"),
        F.greatest(F.col("move_unit_price"), F.col("wh_code_clean_len")).alias("greatest_move_unit_price_wh_code_clean_len"),
        (F.col("capacity") - F.col("wh_code_clean_len")).alias("capacity_minus_wh_code_clean_len"),
    )
)

grouped = (
    tagged
    .groupBy(
        "move_operator",
        F.date_trunc("month", "move_moved_at_day").alias("move_moved_at_day_month"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_citys"),
        F.round(F.sum("capacity") / F.nullif(F.sum("greatest_move_unit_price_wh_code_clean_len"), F.lit(0)), 3).alias("capacity_to_greatest_move_unit_price_wh_code_cle"),
        F.sum(F.when(F.col("city").isin("Kuala Lumpur", "Singapore"), F.col("greatest_move_unit_price_wh_code_clean_len")).otherwise(0)).alias("greatest_move_unit_price_wh_code_clean_len_city_"),
        F.countDistinct("move_operator").alias("distinct_move_operators"),
        F.array_sort(F.collect_set("wh_code")).alias("wh_code_list"),
    )
)

result = (
    grouped
    .select(
        "move_operator",
        "distinct_citys",
        "greatest_move_unit_price_wh_code_clean_len_city_",
        "distinct_move_operators",
        F.concat(F.col("move_operator"), F.lit("#"), F.col("distinct_citys").cast("string")).alias("move_operator_ref"),
    )
    .orderBy("move_operator")
)
