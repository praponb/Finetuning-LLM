from pyspark.sql import functions as F

recent = (
    df_warehouses
    .filter(F.col("city").isin("Kuala Lumpur", "Singapore"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("unit_price").alias("min_move_unit_price"),
    )
)

with_stats = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.min_move_unit_price",
    )
)

derived = (
    with_stats
    .filter(F.lower(F.col("wh_code")).like("%ul%"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        "move_count",
        "min_move_unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
    )
)

result = (
    derived
    .cube("move_count", "city")
    .agg(
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.sum("move_count").alias("total_move_count"),
    )
    .orderBy(F.col("move_count").asc_nulls_last(), F.col("city").asc_nulls_last())
)
