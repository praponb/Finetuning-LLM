from pyspark.sql import functions as F

filtered_warehouses = (
    df_warehouses
    .filter(~F.col("city").isin("Bangkok", "Singapore"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.add_months(F.col("opened_on"), 3).alias("opened_on_plus_3m"),
        F.when(F.col("city").isin("Singapore", "Kuala Lumpur"), "group_a").when(F.col("city") == "Bangkok", "group_b").otherwise("other").alias("city_group"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator").isin("op_ann", "op_dew", "op_bee"), 1)).alias("move_operator_count"),
    )
)

with_stats = (
    filtered_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

city_stats = (
    with_stats
    .groupBy("city")
    .agg(F.avg("move_count").alias("avg_move_count"))
)

outliers = (
    with_stats.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.move_count") > F.col("g.avg_move_count"))
    .select(
        "s.*",
        "g.avg_move_count",
        F.round(F.col("s.move_count") - F.col("g.avg_move_count"), 2).alias("move_count_minus_avg"),
    )
)

annotated = (
    outliers
    .select(
        "*",
        F.concat_ws(": ", F.col("city"), F.col("city_group")).alias("city_city_group_label"),
        F.abs(F.col("capacity_filled") - 9550).alias("capacity_filled_distance"),
        F.trunc(F.col("opened_on_plus_3m"), "MM").alias("opened_on_plus_3m_month_start"),
    )
)

result = (
    annotated
    .select("id", "move_count_minus_avg", "avg_move_count")
    .unpivot(
        ids=["id"],
        values=["move_count_minus_avg", "avg_move_count"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
